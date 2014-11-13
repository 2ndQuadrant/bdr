#include "postgres.h"

#include "bdr.h"
#include "bdr_label.h"

#include "miscadmin.h"

#include "access/relscan.h"
#include "access/skey.h"

#include "catalog/pg_database.h"
#include "catalog/pg_shseclabel.h"

#include "commands/dbcommands.h"

#include "postmaster/bgworker.h"

#include "lib/stringinfo.h"

#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/ipc.h"

#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"

/*
 * TODO DYNCONF Because perdb workers are currently created after
 * the apply workers in shmem, we need to do some temporary extra
 * work to set them up.
 */
static int
count_connections_for_db(const char * dbname)
{
	int i;

	int nnodes = 0;

	for (i = 0; i < bdr_max_workers; i++)
	{
		BdrApplyWorker *apply;
		BdrWorker *entry = &BdrWorkerCtl->slots[i];

		if (entry->worker_type != BDR_WORKER_APPLY)
			continue;

		apply = &entry->worker_data.apply_worker;

		if (strcmp(NameStr(apply->dbname), dbname) != 0)
			continue;

		nnodes++;
		break;
	}

	return nnodes;
}

/*
 * Register a new perdb worker for the named database, assigning it to the free
 * shmem slot identified by worker_slot_number.
 *
 * This is called by the supervisor during startup, and by user backends when
 * the first connection is added for a database.
 */
void
bdr_register_perdb_worker(const char * dbname, int worker_slot_number)
{
	BackgroundWorkerHandle *bgw_handle;
	BackgroundWorker		bgw;
	BdrWorker			   *worker;
	BdrPerdbWorker		   *perdb;

	Assert(LWLockHeldByMe(BdrWorkerCtl->lock));

	worker = &BdrWorkerCtl->slots[worker_slot_number];
	worker->worker_type = BDR_WORKER_PERDB;

	perdb = &worker->worker_data.perdb_worker;

	strncpy(NameStr(perdb->dbname),
			dbname, NAMEDATALEN);
	NameStr(perdb->dbname)[NAMEDATALEN-1] = '\0';
	perdb->nnodes = count_connections_for_db(dbname);
	perdb->seq_slot = worker_slot_number;

	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_main = NULL;
	strncpy(bgw.bgw_library_name, BDR_LIBRARY_NAME, BGW_MAXLEN);
	strncpy(bgw.bgw_function_name, "bdr_perdb_worker_main", BGW_MAXLEN);
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "bdr db: %s", dbname);
	bgw.bgw_main_arg = Int32GetDatum(worker_slot_number);

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("Registering BDR worker failed, check prior log messages for details")));
	}
}

/*
 * Check for BDR-enabled DBs and start per-db workers for any that currently
 * lack them.
 *
 * TODO DYNCONF: Handle removal of BDR from DBs
 */
static void
bdr_supervisor_rescan_dbs()
{
	Relation	secrel;
	ScanKeyData	skey[2];
	SysScanDesc scan;
	HeapTuple	secTuple;

	/* 
	 * Scan pg_seclabel looking for entries for pg_database with the bdr label
	 * provider. We'll find all labels for the BDR provider, irrespective
	 * of value.
	 *
	 * The only index present isn't much use for this scan and using it makes
	 * us set up more keys, so do a heap scan.
	 */
	secrel = heap_open(SharedSecLabelRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_shseclabel_classoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(DatabaseRelationId));

	ScanKeyInit(&skey[1],
				Anum_pg_shseclabel_provider,
				BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(BDR_SECLABEL_PROVIDER));

	scan = systable_beginscan(secrel, InvalidOid, false, NULL, 2, &skey[0]);

	/*
	 * We need to scan the shmem segment that tracks BDR workers and possibly
	 * modify it, so lock it.
	 */
    LWLockAcquire(BdrWorkerCtl->lock, LW_SHARED);

	/*
	 * Now examine each label and if there's no worker for the labled
	 * DB already, start one.
	 */
	while (HeapTupleIsValid(secTuple = systable_getnext(scan)))
	{
		FormData_pg_shseclabel *sec;
		bool					found = false;
		int						lowest_free_slot = -1;
		char 				   *label_dbname;
		int						i;

 		sec = (FormData_pg_shseclabel*) GETSTRUCT(secTuple);

		/*
		 * The per-db workers are mapped by name not oid, and that's necessary
		 * because the bgworker API requires that databases be identified by
		 * name.
		 *
		 * Look up the name of the DB with this OID and compare it. It's a bit slow,
		 * but we aren't doing this much.
		 *
		 * FIXME: Currently if a database is renamed, you'll have to restart
		 * PostgreSQL before BDR notices.
		 */
		label_dbname = get_database_name(sec->objoid);

		/*
		 * TODO DYNCONF: Right now the label *value* is completely ignored.
		 * Instead we should probably be parsing it as json so we can do
		 * useful things with it later.
		 */

		/*
		 * Check if we have a per-db worker for this db oid already. Since we're
		 * going to need to start one if we don't find one, also make a note
		 * of the lowest free slot in the workers array.
		 *
		 * This is O(n^2) for n BDR-enabled DBs; to be more scalable we could
		 * accumulate and sort the oids, then do a single scan of the shmem
		 * segment. But really, if you have that many DBs this cost is nothing.
		 */
		for (i = 0; i < bdr_max_workers; i++)
		{
			BdrWorker *worker = &BdrWorkerCtl->slots[i];

			if (worker->worker_type == BDR_WORKER_PERDB)
			{
				BdrPerdbWorker *perdb_worker = &worker->worker_data.perdb_worker;

				if (strcmp(NameStr(perdb_worker->dbname), label_dbname) == 0)
				{
					found = true;
					break;
				}
			}
			else if (lowest_free_slot < 0
					 && worker->worker_type == BDR_WORKER_EMPTY_SLOT)
			{
				lowest_free_slot = i;
			}
		}

		if (!found)
		{
			if (lowest_free_slot < 0)
			{
				/*
				 * TODO DYNCONF: Allocate a dynamic shmem segment with a continuation
				 * pointer to store more workers instead of bailing out, i.e.
				 * get rid of bdr_max_workers.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						 errmsg("Not enough free BDR worker slots, increase bdr_max_workers")));
			}

			/* No perdb worker exists for this DB, make one */
			bdr_register_perdb_worker(label_dbname, lowest_free_slot);
		}

		pfree(label_dbname);
	}

	LWLockRelease(BdrWorkerCtl->lock);

	systable_endscan(scan);
	heap_close(secrel, AccessShareLock);
}


/*
 * The BDR supervisor is a static bgworker that serves as the master/supervisor
 * for all BDR workers. It exists so that BDR can be enabled and disabled
 * dynamically for databases.
 * 
 * It is responsible for identifying BDR-enabled databases at startup and
 * launching their dynamic per-db workers. It should do as little else as
 * possible, as it'll run when BDR is in shared_preload_libraries whether
 * or not it's otherwise actually in use.
 *
 * The supervisor worker has no access to any database.
 */
void
bdr_supervisor_worker_main(Datum main_arg)
{
	StringInfoData		si;

	initStringInfo(&si);

	Assert( DatumGetInt32(main_arg) == 0);
	Assert(IsBackgroundWorker);

	pqsignal(SIGHUP, bdr_sighup);
	pqsignal(SIGTERM, bdr_sigterm);
	BackgroundWorkerUnblockSignals();

	/*
	 * Call InitPostgres(...) with null dbname and InvalidOid dboid.
	 * 
	 * This will permit us to query shared relations without requiring
	 * a connection to any specific database.
	 */
	BackgroundWorkerInitializeConnection(NULL, NULL);

	initStringInfo(&si);

	appendStringInfo(&si, "bdr supervisor");
	SetConfigOption("application_name", si.data, PGC_USERSET, PGC_S_SESSION);

	bdr_supervisor_rescan_dbs();

	for (;;) {
		int rc;

		/*
		 * After startup the supervisor doesn't currently have anything to do,
		 * so it can just go to sleep on its latch. It could exit after running
		 * startup, but we're expecting to need it to do other things down the
		 * track, so might as well keep it alive...
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   180000L);

		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/*
		 * TODO DYNCONF: We should probably react to SIGHUP and re-run
		 * bdr_supervisor_rescan_dbs() here.
		 */
	}

	/*
	 * XXX TODO FIXME DYNCONF
	 *
	 * Change is_bdr_db to use seclabels
	 * Remove old perdb registration code
	 * Move perdb into separate C file
	 * Add the code to security-label DBs when BDR activated
	 * Add connection tracking table to bdr extension
	 * add connection manip functions
	 * have connection manip functions send signals, do seclabels
	 *
	 * Keep track of bgworkers we registered?
	 *
	 * handle drop/removal
	 */
}

/*
 * Register the BDR supervisor bgworker, which will start all the
 * per-db workers.
 *
 * Called in postmaster context from _PG_init.
 *
 * The supervisor is guaranteed to be assigned the first shmem slot in our
 * workers shmem array. This is vital because at this point shemem isn't
 * allocated yet, so all we can do is tell the supervisor worker its shmem slot
 * number then actually populate that slot when the postmaster runs our shmem
 * init callback later.
 */
void
bdr_supervisor_register()
{
	BackgroundWorker bgw;

	Assert(IsPostmasterEnvironment && !IsUnderPostmaster);

	/* 
	 * The supervisor worker accesses shared relations, but does not connect to
	 * any specific database. We still have to flag it as using a connection in
	 * the bgworker API.
	 */
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_main = NULL;
	strncpy(bgw.bgw_library_name, BDR_LIBRARY_NAME, BGW_MAXLEN);
	strncpy(bgw.bgw_function_name, "bdr_supervisor_worker_main", BGW_MAXLEN);
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "bdr supervisor");
	bgw.bgw_main_arg = Int32GetDatum(0); /* unused */

	RegisterBackgroundWorker(&bgw);
}
