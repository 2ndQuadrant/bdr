#include "postgres.h"

#include "bdr.h"
#include "bdr_label.h"

#include "miscadmin.h"

#include "access/relscan.h"
#include "access/skey.h"
#include "access/xact.h"

#include "catalog/objectaddress.h" //XXX DYNCONF this is temporary
#include "catalog/pg_database.h"
#include "catalog/pg_shseclabel.h"

#include "commands/dbcommands.h"
#include "commands/seclabel.h" //XXX DYNCONF this is temporary

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

static void bdr_copy_labels_from_config(void);
static int count_connections_for_db(const char * dbname);

/*
 * Register a new perdb worker for the named database.
 *
 * This is called by the supervisor during startup, and by user backends when
 * the first connection is added for a database.
 */
void
bdr_register_perdb_worker(const char * dbname)
{
	BackgroundWorkerHandle *bgw_handle;
	BackgroundWorker		bgw;
	BdrWorker			   *worker;
	BdrPerdbWorker		   *perdb;
	unsigned int			worker_slot_number;

	elog(DEBUG2, "Registering per-db worker for %s", dbname);

	Assert(LWLockHeldByMe(BdrWorkerCtl->lock));

	/* We already hold the shmem lock here */
	worker = bdr_worker_shmem_alloc(
				BDR_WORKER_PERDB,
				&worker_slot_number
			);

	perdb = &worker->worker_data.perdb_worker;

	strncpy(NameStr(perdb->dbname),
			dbname, NAMEDATALEN);
	NameStr(perdb->dbname)[NAMEDATALEN-1] = '\0';
	perdb->nnodes = count_connections_for_db(dbname);
#ifdef BUILDING_BDR
	perdb->seq_slot = bdr_sequencer_get_next_free_slot(); //XXX DYNCONF Temporary
#endif

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

	elog(DEBUG2, "Registered per-db worker for %s successfully", dbname);
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

	elog(DEBUG1, "Supervisor scanning for BDR-enabled databases");

	StartTransactionCommand();

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
	 *
	 * We have to take an exclusive lock in case we need to modify it,
	 * otherwise we'd be faced with a lock upgrade.
	 */
    LWLockAcquire(BdrWorkerCtl->lock, LW_EXCLUSIVE);

	/*
	 * Now examine each label and if there's no worker for the labled
	 * DB already, start one.
	 */
	while (HeapTupleIsValid(secTuple = systable_getnext(scan)))
	{
		FormData_pg_shseclabel *sec;
		bool					found = false;
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

		elog(DEBUG1, "Found BDR-enabled database %s (oid=%i)",
			 label_dbname, sec->objoid);

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
		}

		elog(DEBUG2, "worker exists? %s", (found?"true":"false")); //XXX

		if (!found)
		{
			elog(DEBUG2, "registering..."); //XXX
			/* No perdb worker exists for this DB, make one */
			bdr_register_perdb_worker(label_dbname);
		}

		pfree(label_dbname);
	}

	elog(DEBUG2, "Registered all per-db workers");

	LWLockRelease(BdrWorkerCtl->lock);

	systable_endscan(scan);
	heap_close(secrel, AccessShareLock);

	CommitTransactionCommand();

	elog(DEBUG2, "Finished scanning for BDR-enabled databases");
}

/*
 * Create the database the supervisor remains connected
 * to, a DB with no user connections permitted.
 *
 * This is a workaorund for the inability to use pg_shseclabel
 * without a DB connection; see comments in bdr_supervisor_main
 */
static void
bdr_supervisor_createdb()
{
	Oid dboid;

	StartTransactionCommand();

	/* If the DB already exists, no need to create it */
	dboid = get_database_oid("bdr", true);

	if (dboid == InvalidOid)
	{
		CreatedbStmt stmt;
		DefElem de_template;
		DefElem de_connlimit;

		de_template.defname = "template";
		de_template.type = T_String;
		de_template.arg = (Node*) makeString("template1");

		de_connlimit.defname = "connectionlimit";
		de_template.type = T_Integer;
		de_connlimit.arg = (Node*) makeInteger(1);

		stmt.dbname = "bdr";
		stmt.options = list_make2(&de_template, &de_connlimit);

		dboid = createdb(&stmt);
		
		if (dboid == InvalidOid)
			elog(ERROR, "Failed to create 'bdr' DB");

		elog(LOG, "Created database 'bdr' (oid=%i) during BDR startup", dboid);
	}
	else
	{
		elog(DEBUG3, "Database 'bdr' (oid=%i) already exists, not creating", dboid);
	}

	CommitTransactionCommand();

	Assert(dboid != InvalidOid);
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
	 *
	 * XXX DYNCONF for this to work you have to patch src/backend/utils/init/postinit.c,
	 * changing
	 * 	if (IsAutoVacuumLauncherProcess())
	 * to
	 *  if (IsAutoVacuumLauncherProcess() || (in_dbname == NULL && dboid == InvalidOid && !bootstrap))
	 * at line 620, in InitPostgres
	 *
	 * Even with that change we still fail, because you can't access pg_class oids w/o a DB:
	 *
	 *   FATAL:  XX000: cannot read pg_class without having selected a database
	 *   LOCATION:  ScanPgRelation, relcache.c:316
	 *
	 * so right now we're just going to require a DB to exist. 
	 */
	/* BackgroundWorkerInitializeConnection(NULL, NULL); *///
	
	/*
	 * We can't connect to no db, because InitPostgres only lets the autovacuum
	 * manager do that.
	 * 
	 * See http://www.postgresql.org/message-id/flat/CA+M2pVU4GoHW2wwvE1jd32pYKYwXUpEuT=s1Ji7YAgnLFFARzA@mail.gmail.com
	 *
	 * XXX DYNCONF
	 */

	/*
	 * Because we can't connect to "no database" because of the issues outlined
	 * above, we must connect to a usable one, like template1, then use it to
	 * create a dummy database to operate in.
	 *
	 * We can't use template0, as dataisconn must be enabled for the DB. We don't use
	 * postgres because it might not exist, wheras 'template1' is pretty much guaranteed
	 * to.
	 *
	 * Once created we set a shmem flag and restart so we know we can connect
	 * to the newly created database.
	 */
	if (!BdrWorkerCtl->is_supervisor_restart)
	{
		BackgroundWorkerInitializeConnection("template1", NULL);
		bdr_supervisor_createdb();

		/*
		 * XXX DYNCONF Because config isn't fully migrated yet, we need to
		 * scan the configured list of DBs and apply security labels for each
		 * during startup so that "make check" can actually work.
		 */
		bdr_copy_labels_from_config();

		BdrWorkerCtl->is_supervisor_restart = true;
		elog(DEBUG1, "BDR supervisor restarting to connect to 'bdr' DB");
		proc_exit(1);
	}

	BackgroundWorkerInitializeConnection("bdr", NULL);
	elog(DEBUG1, "BDR supervisor connected to DB 'bdr'");

	initStringInfo(&si);

	appendStringInfo(&si, "bdr supervisor");
	SetConfigOption("application_name", si.data, PGC_USERSET, PGC_S_SESSION);

	bdr_supervisor_rescan_dbs();

	while (!got_SIGTERM)
	{
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

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * TODO DYNCONF: We should probably react to SIGHUP and re-run
		 * bdr_supervisor_rescan_dbs() here.
		 */
	}

	proc_exit(0);

	/*
	 * XXX TODO FIXME DYNCONF
	 *
	 * Change is_bdr_db to use seclabels
	 *
	 * add connection manip functions
	 * have connection manip functions send signals, do seclabels
	 * fix crash/restart repeat worker launch
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
	bgw.bgw_restart_time = 1;
	bgw.bgw_notify_pid = 0;
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "bdr supervisor");
	bgw.bgw_main_arg = Int32GetDatum(0); /* unused */

	RegisterBackgroundWorker(&bgw);
}




/***
 * Code after this point is throwaway code for the interim
 * half-dynamic configuration support.
 ***/

/*
 * TODO DYNCONF For each DB configured, apply a security label.
 *
 * Temporary code during the transition to dynamic config.
 *
 * Don't care in the slightest if it's slow and ugly, it's
 * throwaway code.
 */
static void
bdr_copy_labels_from_config()
{
	char **distinct_dbnames;
	int n_distinct_dbnames = 0, i, j;

	distinct_dbnames = (char**)palloc0(sizeof(char*) * bdr_max_workers);

	StartTransactionCommand();

	for (i = 0; i < bdr_max_workers; i++)
	{
		BdrConnectionConfig *cfg = bdr_connection_configs[i];
		bool found = false;

		if (cfg == NULL)
			continue;

		/* Does this worker's db already exist in the namelist? */
		for (j = 0; j < n_distinct_dbnames; j++)
		{
			if (strcmp(cfg->dbname, distinct_dbnames[j]) == 0)
			{
				found = true;
				break;
			}
		}
		if (!found)
		{
			elog(LOG, "Found distinct dbname %s", cfg->dbname);
			distinct_dbnames[n_distinct_dbnames++] = cfg->dbname;
		}
	}

	elog(LOG, "XXX Found %i distinct dbnames", n_distinct_dbnames);

	/* Right-o, now set labels for each */
	for (i = 0; i < n_distinct_dbnames; i++)
	{
		ObjectAddress addr;
		addr.objectId = get_database_oid(distinct_dbnames[i], false);
		addr.classId = DatabaseRelationId;
		addr.objectSubId = 0;

		SetSecurityLabel(&addr, "bdr", "enabled");
	}

	CommitTransactionCommand();

	pfree(distinct_dbnames);
}

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
		BdrConnectionConfig *cfg = bdr_connection_configs[i];
		if (cfg == NULL)
			continue;

		if (strcmp(cfg->dbname, dbname) != 0)
			continue;

		nnodes++;
		break;
	}

	return nnodes;
}
