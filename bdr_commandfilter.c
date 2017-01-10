/* -------------------------------------------------------------------------
 *
 * bdr_commandfilter.c
 *		prevent execution of utility commands not yet or never supported
 *
 *
 * Copyright (C) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		bdr_commandfilter.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "bdr.h"
#include "bdr_locks.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/seqam.h"

#include "catalog/namespace.h"

#include "commands/dbcommands.h"
#include "commands/event_trigger.h"
#include "commands/extension.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"

/* For the client auth filter */
#include "libpq/auth.h"

#include "parser/parse_utilcmd.h"

#include "replication/replication_identifier.h"

#include "storage/standby.h"

#include "tcop/utility.h"

#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
* bdr_commandfilter.c: a ProcessUtility_hook to prevent a cluster from running
* commands that BDR does not yet support.
*/

static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;

static ClientAuthentication_hook_type next_ClientAuthentication_hook = NULL;

/* GUCs */
bool bdr_permit_unsafe_commands = false;

static void error_unsupported_command(const char *cmdtag) __attribute__((noreturn));

/*
* Check the passed rangevar, locking it and looking it up in the cache
* then determine if the relation requires logging to WAL. If it does, then
* right now BDR won't cope with it and we must reject the operation that
* touches this relation.
*/
static void
error_on_persistent_rv(RangeVar *rv,
					   const char *cmdtag,
					   LOCKMODE lockmode,
					   bool missing_ok)
{
	bool		needswal;
	Relation	rel;

	if (rv == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Unqualified command %s is unsafe with BDR active.",
						cmdtag)));

	rel = heap_openrv_extended(rv, lockmode, missing_ok);

	if (rel != NULL)
	{
		needswal = RelationNeedsWAL(rel);
		heap_close(rel, lockmode);
		if (needswal)
			ereport(ERROR,
				 (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				  errmsg("%s may only affect UNLOGGED or TEMPORARY tables " \
						 "when BDR is active; %s is a regular table",
						 cmdtag, rv->relname)));
	}
}

static void
error_unsupported_command(const char *cmdtag)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("%s is not supported when bdr is active",
					cmdtag)));
}

static bool ispermanent(const char persistence)
{
	/* In case someone adds a new type we don't know about */
	Assert(persistence == RELPERSISTENCE_TEMP
			|| persistence == RELPERSISTENCE_UNLOGGED
			|| persistence == RELPERSISTENCE_PERMANENT);

	return persistence == RELPERSISTENCE_PERMANENT;
}

static void
filter_CreateStmt(Node *parsetree,
				  char *completionTag)
{
	CreateStmt *stmt;
	ListCell   *cell;
	bool		with_oids = default_with_oids;

	stmt = (CreateStmt *) parsetree;

	if (stmt->ofTypename != NULL)
		error_unsupported_command("CREATE TABLE ... OF TYPE");

	/* verify WITH options */
	foreach(cell, stmt->options)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		/* reject WITH OIDS */
		if (def->defnamespace == NULL &&
			pg_strcasecmp(def->defname, "oids") == 0)
		{
			with_oids = defGetBoolean(def);
		}
	}

	if (with_oids)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Tables WITH OIDs are not supported with bdr")));
	}

	/* verify table elements */
	foreach(cell, stmt->tableElts)
	{
		Node	   *element = lfirst(cell);

		if (nodeTag(element) == T_Constraint)
		{
			Constraint *con = (Constraint *) element;

			if (con->contype == CONSTR_EXCLUSION &&
				ispermanent(stmt->relation->relpersistence))
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("EXCLUDE constraints are unsafe with BDR active")));
			}
		}
	}
}

static void
filter_AlterTableStmt(Node *parsetree,
					  char *completionTag,
					  const char *queryString,
					  BDRLockType *lock_type)
{
	AlterTableStmt *astmt;
	ListCell   *cell,
			   *cell1;
	bool		hasInvalid;
	List	   *stmts;
	Oid			relid;
	LOCKMODE	lockmode;

	astmt = (AlterTableStmt *) parsetree;
	hasInvalid = false;

	/*
	 * Can't use AlterTableGetLockLevel(astmt->cmds);
	 * Otherwise we deadlock between the global DDL locks and DML replay.
	 * ShareUpdateExclusiveLock should be enough to block DDL but not DML.
	 */
	lockmode = ShareUpdateExclusiveLock;
	relid = AlterTableLookupRelation(astmt, lockmode);

	stmts = transformAlterTableStmt(relid, astmt, queryString);

	foreach(cell, stmts)
	{
		Node	   *node = (Node *) lfirst(cell);
		AlterTableStmt *at_stmt;

		/*
		 * we ignore all nodes which are not AlterTableCmd statements since
		 * the standard utility hook will recurse and thus call our handler
		 * again
		 */
		if (!IsA(node, AlterTableStmt))
			continue;

		at_stmt = (AlterTableStmt *) node;

		foreach(cell1, at_stmt->cmds)
		{
			AlterTableCmd *stmt = (AlterTableCmd *) lfirst(cell1);

			switch (stmt->subtype)
			{
					/*
					 * allowed for now:
					 */
				case AT_AddColumn:
					{
						ColumnDef  *def = (ColumnDef *) stmt->def;
						ListCell   *cell;

						/*
						 * Error out if there's a default for the new column,
						 * that requires a table rewrite which might be
						 * nondeterministic.
						 */
						if (def->raw_default != NULL ||
							def->cooked_default != NULL)
						{
							error_on_persistent_rv(
												   astmt->relation,
									"ALTER TABLE ... ADD COLUMN ... DEFAULT",
									               lockmode,
												   astmt->missing_ok);
						}

						/*
						 * Column defaults can also be represented as
						 * constraints.
						 */
						foreach(cell, def->constraints)
						{
							Constraint *con;

							Assert(IsA(lfirst(cell), Constraint));
							con = (Constraint *) lfirst(cell);

							if (con->contype == CONSTR_DEFAULT)
								error_on_persistent_rv(
													   astmt->relation,
									"ALTER TABLE ... ADD COLUMN ... DEFAULT",
										               lockmode,
													   astmt->missing_ok);
						}
					}
				case AT_AddIndex: /* produced by for example ALTER TABLE … ADD
								   * CONSTRAINT … PRIMARY KEY */
				case AT_DropColumn:
				case AT_DropNotNull:
				case AT_SetNotNull:
				case AT_ColumnDefault:	/* ALTER COLUMN DEFAULT */

				case AT_ClusterOn:		/* CLUSTER ON */
				case AT_DropCluster:	/* SET WITHOUT CLUSTER */
				case AT_ChangeOwner:
				case AT_SetStorage:
					lock_type = BDR_LOCK_DDL;
					break;

				case AT_SetRelOptions:	/* SET (...) */
				case AT_ResetRelOptions:		/* RESET (...) */
				case AT_ReplaceRelOptions:		/* replace reloption list */
				case AT_ReplicaIdentity:
					break;

				case AT_DropConstraint:
					break;

				case AT_SetTableSpace:
					break;

				case AT_AddConstraint:
				case AT_ProcessedConstraint:
					if (IsA(stmt->def, Constraint))
					{
						Constraint *con = (Constraint *) stmt->def;

						if (con->contype == CONSTR_EXCLUSION)
							error_on_persistent_rv(astmt->relation,
								"ALTER TABLE ... ADD CONSTRAINT ... EXCLUDE",
									               lockmode,
												   astmt->missing_ok);
					}
					break;

				case AT_ValidateConstraint: /* VALIDATE CONSTRAINT */
					break;

				case AT_AlterConstraint:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ALTER CONSTRAINT",
							               lockmode,
										   astmt->missing_ok);
					break;

				case AT_AddIndexConstraint:
					/* no deparse support */
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ADD CONSTRAINT USING INDEX",
							               lockmode,
										   astmt->missing_ok);
					break;

				case AT_AlterColumnType:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ALTER COLUMN TYPE",
							               lockmode,
										   astmt->missing_ok);
					break;

				case AT_AlterColumnGenericOptions:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ALTER COLUMN OPTIONS",
							               lockmode,
										   astmt->missing_ok);
					break;

				case AT_AddOids:
				case AT_DropOids:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... SET WITH[OUT] OIDS",
							               lockmode,
										   astmt->missing_ok);
					break;

				case AT_EnableTrig:
				case AT_DisableTrig:
				case AT_EnableTrigUser:
				case AT_DisableTrigUser:
					/*
					 * It's safe to ALTER TABLE ... ENABLE|DISABLE TRIGGER
					 * without blocking concurrent writes.
					 */
					lock_type = BDR_LOCK_DDL;
					break;

				case AT_EnableAlwaysTrig:
				case AT_EnableReplicaTrig:
				case AT_EnableTrigAll:
				case AT_DisableTrigAll:
					/*
					 * Since we might fire replica triggers later and that
					 * could affect replication, continue to take a write-lock
					 * for them.
					 */
					break;

				case AT_EnableRule:
				case AT_EnableAlwaysRule:
				case AT_EnableReplicaRule:
				case AT_DisableRule:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ENABLE|DISABLE [ALWAYS|REPLICA] RULE",
							               lockmode,
										   astmt->missing_ok);
					break;

				case AT_AddInherit:
				case AT_DropInherit:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... [NO] INHERIT",
							               lockmode,
										   astmt->missing_ok);
					break;

				case AT_AddOf:
				case AT_DropOf:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... [NOT] OF",
							               lockmode,
										   astmt->missing_ok);
					break;

				case AT_SetStatistics:
					break;
				case AT_SetOptions:
				case AT_ResetOptions:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ALTER COLUMN ... SET STATISTICS|(...)",
							               lockmode,
										   astmt->missing_ok);
					break;

				case AT_GenericOptions:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... SET (...)",
							               lockmode,
										   astmt->missing_ok);
					break;

				default:
					hasInvalid = true;
					break;
			}
		}
	}

	if (hasInvalid)
		error_on_persistent_rv(astmt->relation,
							   "This variant of ALTER TABLE",
				               lockmode,
							   astmt->missing_ok);
}

static void
filter_CreateSeqStmt(Node *parsetree)
{
	ListCell	   *param;
	CreateSeqStmt  *stmt;

	stmt = (CreateSeqStmt *) parsetree;

	if (stmt->accessMethod == NULL || strcmp(stmt->accessMethod, "bdr") != 0)
		return;

	foreach(param, stmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(param);
		if (strcmp(defel->defname, "owned_by") != 0 &&
			strcmp(defel->defname, "start") != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("CREATE SEQUENCE ... %s is not supported for bdr sequences",
					defel->defname)));
	}
}

static void
filter_AlterSeqStmt(Node *parsetree)
{
	Oid				seqoid;
	ListCell	   *param;
	AlterSeqStmt   *stmt;
	Oid				seqamid;
	HeapTuple		ctup;
	Form_pg_class	pgcform;

	stmt = (AlterSeqStmt *) parsetree;

	seqoid = RangeVarGetRelid(stmt->sequence, AccessShareLock, true);

	if (seqoid == InvalidOid)
		return;

	seqamid = get_seqam_oid("bdr", true);

	/* No bdr sequences? */
	if (seqamid == InvalidOid)
		return;

	/* Fetch a tuple to check for relam */
	ctup = SearchSysCache1(RELOID, ObjectIdGetDatum(seqoid));
	if (!HeapTupleIsValid(ctup))
		elog(ERROR, "pg_class entry for sequence %u unavailable",
						seqoid);
	pgcform = (Form_pg_class) GETSTRUCT(ctup);

	/* Not bdr sequence */
	if (pgcform->relam != seqamid)
	{
		ReleaseSysCache(ctup);
		return;
	}

	ReleaseSysCache(ctup);

	foreach(param, stmt->options)
	{
		DefElem    *defel = (DefElem *) lfirst(param);
		if (strcmp(defel->defname, "owned_by") != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("ALTER SEQUENCE ... %s is not supported for bdr sequences",
					defel->defname)));
	}
}

static void
filter_CreateTableAs(Node *parsetree)
{
	CreateTableAsStmt *stmt;
	stmt = (CreateTableAsStmt *) parsetree;

	if (ispermanent(stmt->into->rel->relpersistence))
		error_unsupported_command(CreateCommandTag(parsetree));
}

static bool
statement_affects_only_nonpermanent(Node *parsetree)
{
	switch (nodeTag(parsetree))
	{
		case T_CreateTableAsStmt:
			{
				CreateTableAsStmt *stmt = (CreateTableAsStmt *) parsetree;
				return !ispermanent(stmt->into->rel->relpersistence);
			}
		case T_CreateStmt:
			{
				CreateStmt *stmt = (CreateStmt *) parsetree;
				return !ispermanent(stmt->relation->relpersistence);
			}
		case T_DropStmt:
			{
				DropStmt *stmt = (DropStmt *) parsetree;
				ListCell   *cell;

				/*
				 * It doesn't make any sense to drop temporary tables
				 * concurrently.
				 */
				if (stmt->concurrent)
					return false;

				/* Figure out if only temporary objects are affected. */

				/*
				 * Only do this for temporary relations and indexes, not
				 * other objects for now.
				 */
				switch (stmt->removeType)
				{
					case OBJECT_INDEX:
					case OBJECT_TABLE:
					case OBJECT_SEQUENCE:
					case OBJECT_VIEW:
					case OBJECT_MATVIEW:
					case OBJECT_FOREIGN_TABLE:
						break;
					default:
						return false;

				}

				/* Now check each dropped relation. */
				foreach(cell, stmt->objects)
				{
					Oid			relOid;
					RangeVar   *rv = makeRangeVarFromNameList((List *) lfirst(cell));
					Relation	rel;
					bool		istemp;

					relOid = RangeVarGetRelidExtended(rv,
													  AccessExclusiveLock,
													  stmt->missing_ok,
													  false,
													  NULL,
													  NULL);
					if (relOid == InvalidOid)
						continue;

					/*
					 * If a schema name is not provided, check to see if the session's
					 * temporary namespace is first in the search_path and if a relation
					 * with the same Oid is in the current session's "pg_temp" schema. If
					 * so, we can safely assume that the DROP statement will refer to this
					 * object, since the pg_temp schema is session-private.
					 */
					if (rv->schemaname == NULL)
					{
						Oid tempNamespaceOid, tempRelOid;
						List* searchPath;
						bool foundtemprel;

						foundtemprel = false;
						tempNamespaceOid = LookupExplicitNamespace("pg_temp", true);
						if (tempNamespaceOid == InvalidOid)
							return false;
						searchPath = fetch_search_path(true);
						if (searchPath != NULL)
						{
							ListCell* i;

							foreach(i, searchPath)
							{
								if (lfirst_oid(i) != tempNamespaceOid)
									break;
								tempRelOid = get_relname_relid(rv->relname, tempNamespaceOid);
								if (tempRelOid != relOid)
									break;
								foundtemprel = true;
								break;
							}
							list_free(searchPath);
						}
						if (!foundtemprel)
							return false;
					}

					if (stmt->removeType != OBJECT_INDEX)
					{
						rel = relation_open(relOid, AccessExclusiveLock);
						istemp = !ispermanent(rel->rd_rel->relpersistence);
						relation_close(rel, NoLock);
					}
					else
					{
						rel = index_open(relOid, AccessExclusiveLock);
						istemp = !ispermanent(rel->rd_rel->relpersistence);
						index_close(rel, NoLock);
					}

					if (!istemp)
						return false;
				}
				return true;
				break;
			}
		case T_IndexStmt:
			{
				IndexStmt *stmt = (IndexStmt *) parsetree;
				return !ispermanent(stmt->relation->relpersistence);
			}
		/* FIXME: Add more types of statements */
		default:
			break;
	}
	return false;
}

static bool
allowed_on_read_only_node(Node *parsetree)
{
	/*
	 * This list is copied verbatim from check_xact_readonly
	 * we only do different action on it. */
	switch (nodeTag(parsetree))
	{
		case T_AlterDatabaseStmt:
		case T_AlterDatabaseSetStmt:
		case T_AlterDomainStmt:
		case T_AlterFunctionStmt:
		case T_AlterRoleStmt:
		case T_AlterRoleSetStmt:
		case T_AlterObjectSchemaStmt:
		case T_AlterOwnerStmt:
		case T_AlterSeqStmt:
		case T_AlterTableMoveAllStmt:
		case T_AlterTableStmt:
		case T_RenameStmt:
		case T_CommentStmt:
		case T_DefineStmt:
		case T_CreateCastStmt:
		case T_CreateEventTrigStmt:
		case T_AlterEventTrigStmt:
		case T_CreateConversionStmt:
		case T_CreatedbStmt:
		case T_CreateDomainStmt:
		case T_CreateFunctionStmt:
		case T_CreateRoleStmt:
		case T_IndexStmt:
		case T_CreatePLangStmt:
		case T_CreateOpClassStmt:
		case T_CreateOpFamilyStmt:
		case T_AlterOpFamilyStmt:
		case T_RuleStmt:
		case T_CreateSchemaStmt:
		case T_CreateSeqStmt:
		case T_CreateStmt:
		case T_CreateTableAsStmt:
		case T_RefreshMatViewStmt:
		case T_CreateTableSpaceStmt:
		case T_CreateTrigStmt:
		case T_CompositeTypeStmt:
		case T_CreateEnumStmt:
		case T_CreateRangeStmt:
		case T_AlterEnumStmt:
		case T_ViewStmt:
		case T_DropStmt:
		case T_DropdbStmt:
		case T_DropTableSpaceStmt:
		case T_DropRoleStmt:
		case T_GrantStmt:
		case T_GrantRoleStmt:
		case T_AlterDefaultPrivilegesStmt:
		case T_TruncateStmt:
		case T_DropOwnedStmt:
		case T_ReassignOwnedStmt:
		case T_AlterTSDictionaryStmt:
		case T_AlterTSConfigurationStmt:
		case T_CreateExtensionStmt:
		case T_AlterExtensionStmt:
		case T_AlterExtensionContentsStmt:
		case T_CreateFdwStmt:
		case T_AlterFdwStmt:
		case T_CreateForeignServerStmt:
		case T_AlterForeignServerStmt:
		case T_CreateUserMappingStmt:
		case T_AlterUserMappingStmt:
		case T_DropUserMappingStmt:
		case T_AlterTableSpaceOptionsStmt:
		case T_CreateForeignTableStmt:
		case T_SecLabelStmt:
			return statement_affects_only_nonpermanent(parsetree);
		default:
			/* do nothing */
			break;
	}

	return true;
}

static void
bdr_commandfilter_dbname(const char *dbname)
{
	if (strcmp(dbname, BDR_SUPERVISOR_DBNAME) == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("The BDR extension reserves the database name "
						BDR_SUPERVISOR_DBNAME" for its own use"),
				 errhint("Use a different database name")));
	}
}

static void
prevent_drop_extension_bdr(DropStmt *stmt)
{
	ListCell   *cell;

	/* Only interested in DROP EXTENSION */
	if (stmt->removeType != OBJECT_EXTENSION)
		return;

	/* Check to see if the BDR extension is being dropped */
	foreach(cell, stmt->objects)
	{
		ObjectAddress address;
		List	   *objname = lfirst(cell);
		Relation	relation = NULL;

		/* Get an ObjectAddress for the object. */
		address = get_object_address(stmt->removeType,
									 objname, NULL,
									 &relation,
									 AccessExclusiveLock,
									 stmt->missing_ok);

		if (!OidIsValid(address.objectId))
			continue;

		/* for an extension the object name is unqualified */
		Assert(list_length(objname) == 1);

		if (strcmp(strVal(linitial(objname)), "bdr") == 0)
			ereport(ERROR,
					(errmsg("Dropping the BDR extension is prohibited while BDR is active"),
					 errhint("Part this node with bdr.part_by_node_names(...) first, or if appropriate use bdr.remove_bdr_from_local_node(...)")));
	}
}

static void
bdr_commandfilter(Node *parsetree,
				  const char *queryString,
				  ProcessUtilityContext context,
				  ParamListInfo params,
				  DestReceiver *dest,
				  char *completionTag)
{
	/* take strongest lock by default. */
	BDRLockType	lock_type = BDR_LOCK_WRITE;

	/* don't filter in single user mode */
	if (!IsUnderPostmaster)
		goto done;

	/* Only permit VACUUM on the supervisordb, if it exists */
	if (BdrSupervisorDbOid == InvalidOid)
		BdrSupervisorDbOid = bdr_get_supervisordb_oid(true);
		
	if (BdrSupervisorDbOid != InvalidOid
		&& MyDatabaseId == BdrSupervisorDbOid
		&& nodeTag(parsetree) != T_VacuumStmt)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("No commands may be run on the BDR supervisor database")));
	}

	/* extension contents aren't individually replicated */
	if (creating_extension)
		goto done;

	/* don't perform filtering while replaying */
	if (replication_origin_id != InvalidRepNodeId)
		goto done;

	/*
	 * Skip transaction control commands first as the following function
	 * calls might require transaction access.
	 */
	if (nodeTag(parsetree) == T_TransactionStmt)
		goto done;

	/* don't filter if this database isn't using bdr */
	if (!bdr_is_bdr_activated_db(MyDatabaseId))
		goto done;

	/* check for read-only mode */
	if (bdr_local_node_read_only() && !allowed_on_read_only_node(parsetree))
		ereport(ERROR,
				(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
				 errmsg("Cannot run %s on read-only BDR node.",
						CreateCommandTag(parsetree))));

	/* don't filter if explicitly told so */
	if (bdr_permit_unsafe_commands)
		goto done;

	/* commands we skip (for now) */
	switch (nodeTag(parsetree))
	{
		case T_PlannedStmt:
		case T_ClosePortalStmt:
		case T_FetchStmt:
		case T_DoStmt:
		case T_CreateTableSpaceStmt:
		case T_DropTableSpaceStmt:
		case T_AlterTableSpaceOptionsStmt:
		case T_TruncateStmt:
		case T_CommentStmt: /* XXX: we could replicate these */;
		case T_CopyStmt:
		case T_PrepareStmt:
		case T_ExecuteStmt:
		case T_DeallocateStmt:
		case T_GrantStmt: /* XXX: we could replicate some of these these */;
		case T_GrantRoleStmt:
		case T_AlterDatabaseStmt:
		case T_AlterDatabaseSetStmt:
		case T_NotifyStmt:
		case T_ListenStmt:
		case T_UnlistenStmt:
		case T_LoadStmt:
		case T_ClusterStmt: /* XXX: we could replicate these */;
		case T_VacuumStmt:
		case T_ExplainStmt:
		case T_AlterSystemStmt:
		case T_VariableSetStmt:
		case T_VariableShowStmt:
		case T_DiscardStmt:
		case T_CreateEventTrigStmt:
		case T_AlterEventTrigStmt:
		case T_CreateRoleStmt:
		case T_AlterRoleStmt:
		case T_AlterRoleSetStmt:
		case T_DropRoleStmt:
		case T_ReassignOwnedStmt:
		case T_LockStmt:
		case T_ConstraintsSetStmt:
		case T_CheckPointStmt:
		case T_ReindexStmt:
			goto done;

		default:
			break;
	}

	/*
	 * We stop people from creating a DB named BDR_SUPERVISOR_DBNAME if the BDR
	 * extension is installed because we reserve that name, even if BDR isn't
	 * actually active.
	 *
	 */
	switch (nodeTag(parsetree))
	{
		case T_CreatedbStmt:
			bdr_commandfilter_dbname(((CreatedbStmt*)parsetree)->dbname);
			goto done;
		case T_DropdbStmt:
			bdr_commandfilter_dbname(((DropdbStmt*)parsetree)->dbname);
			goto done;
		case T_RenameStmt:
			/*
			 * ALTER DATABASE ... RENAME TO ... is actually a RenameStmt not an
			 * AlterDatabaseStmt. It's handled here for the database target only
			 * then falls through for the other rename object type.
			 */
			if (((RenameStmt*)parsetree)->renameType == OBJECT_DATABASE)
			{
				bdr_commandfilter_dbname(((RenameStmt*)parsetree)->subname);
				bdr_commandfilter_dbname(((RenameStmt*)parsetree)->newname);
			}
			break;

		default:
			break;
	}

	/* statements handled directly in standard_ProcessUtility */
	switch (nodeTag(parsetree))
	{
		case T_DropStmt:
			{
				DropStmt   *stmt = (DropStmt *) parsetree;

				prevent_drop_extension_bdr(stmt);

				if (EventTriggerSupportsObjectType(stmt->removeType))
					break;
				else
					goto done;
			}
		case T_RenameStmt:
			{
				RenameStmt *stmt = (RenameStmt *) parsetree;

				if (EventTriggerSupportsObjectType(stmt->renameType))
					break;
				else
					goto done;
			}
		case T_AlterObjectSchemaStmt:
			{
				AlterObjectSchemaStmt *stmt = (AlterObjectSchemaStmt *) parsetree;

				if (EventTriggerSupportsObjectType(stmt->objectType))
					break;
				else
					goto done;
			}
		case T_AlterOwnerStmt:
			{
				AlterOwnerStmt *stmt = (AlterOwnerStmt *) parsetree;

				lock_type = BDR_LOCK_DDL;

				if (EventTriggerSupportsObjectType(stmt->objectType))
					break;
				else
					goto done;
			}

		default:
			break;
	}

	/* all commands handled by ProcessUtilitySlow() */
	switch (nodeTag(parsetree))
	{
		case T_CreateSchemaStmt:
			lock_type = BDR_LOCK_DDL;
			break;

		case T_CreateStmt:
			filter_CreateStmt(parsetree, completionTag);
			lock_type = BDR_LOCK_DDL;
			break;

		case T_CreateForeignTableStmt:
			lock_type = BDR_LOCK_DDL;
			break;

		case T_AlterTableStmt:
			filter_AlterTableStmt(parsetree, completionTag, queryString, &lock_type);
			break;

		case T_AlterDomainStmt:
			/* XXX: we could support this */
			error_unsupported_command(CreateCommandTag(parsetree));
			break;

		case T_DefineStmt:
			{
				DefineStmt *stmt = (DefineStmt *) parsetree;

				switch (stmt->kind)
				{
					case OBJECT_AGGREGATE:
					case OBJECT_OPERATOR:
					case OBJECT_TYPE:
						break;

					default:
						error_unsupported_command(CreateCommandTag(parsetree));
						break;
				}

				lock_type = BDR_LOCK_DDL;
				break;
			}

		case T_IndexStmt:
			{
				IndexStmt  *stmt;

				stmt = (IndexStmt *) parsetree;

				if (stmt->whereClause && stmt->unique)
					error_on_persistent_rv(stmt->relation,
										   "CREATE UNIQUE INDEX ... WHERE",
										   AccessExclusiveLock, false);

				/*
				 * Non-unique concurrently built indexes can be done in
				 * parallel with writing.
				 */
				if (!stmt->unique && stmt->concurrent)
					lock_type = BDR_LOCK_DDL;

				break;
			}
		case T_CreateExtensionStmt:
			break;

		case T_AlterExtensionStmt:
			/* XXX: we could support some of these */
			error_unsupported_command(CreateCommandTag(parsetree));
			break;

		case T_AlterExtensionContentsStmt:
			error_unsupported_command(CreateCommandTag(parsetree));
			break;

		case T_CreateFdwStmt:
		case T_AlterFdwStmt:
		case T_CreateForeignServerStmt:
		case T_AlterForeignServerStmt:
		case T_CreateUserMappingStmt:
		case T_AlterUserMappingStmt:
		case T_DropUserMappingStmt:
			/* XXX: we should probably support all of these at some point */
			error_unsupported_command(CreateCommandTag(parsetree));
			break;

		case T_CompositeTypeStmt:	/* CREATE TYPE (composite) */
		case T_CreateEnumStmt:		/* CREATE TYPE AS ENUM */
		case T_CreateRangeStmt:		/* CREATE TYPE AS RANGE */
			lock_type = BDR_LOCK_DDL;
			break;

		case T_ViewStmt:	/* CREATE VIEW */
		case T_CreateFunctionStmt:	/* CREATE FUNCTION */
			lock_type = BDR_LOCK_DDL;
			break;

		case T_AlterEnumStmt:
		case T_AlterFunctionStmt:	/* ALTER FUNCTION */
		case T_RuleStmt:	/* CREATE RULE */
			break;

		case T_CreateSeqStmt:
			filter_CreateSeqStmt(parsetree);
			break;

		case T_AlterSeqStmt:
			filter_AlterSeqStmt(parsetree);
			break;

		case T_CreateTableAsStmt:
			filter_CreateTableAs(parsetree);
			break;

		case T_RefreshMatViewStmt:
			/* XXX: might make sense to support or not */
			error_unsupported_command(CreateCommandTag(parsetree));
			break;

		case T_CreateTrigStmt:
			break;

		case T_CreatePLangStmt:
			error_unsupported_command(CreateCommandTag(parsetree));
			break;

		case T_CreateDomainStmt:
			lock_type = BDR_LOCK_DDL;
			break;

		case T_CreateConversionStmt:
			error_unsupported_command(CreateCommandTag(parsetree));
			break;

		case T_CreateCastStmt:
		case T_CreateOpClassStmt:
		case T_CreateOpFamilyStmt:
		case T_AlterOpFamilyStmt:
			error_unsupported_command(CreateCommandTag(parsetree));
			break;

		case T_AlterTSDictionaryStmt:
		case T_AlterTSConfigurationStmt:
			error_unsupported_command(CreateCommandTag(parsetree));
			break;

		case T_DropStmt:
			break;

		case T_RenameStmt:
			{
				RenameStmt *n = (RenameStmt *)parsetree;

				switch(n->renameType)
				{
				case OBJECT_AGGREGATE:
				case OBJECT_COLLATION:
				case OBJECT_CONVERSION:
				case OBJECT_OPCLASS:
				case OBJECT_OPFAMILY:
					error_unsupported_command(CreateCommandTag(parsetree));
					break;

				default:
					break;
				}
			}
			break;

		case T_AlterObjectSchemaStmt:
			error_unsupported_command(CreateCommandTag(parsetree));
			break;

		case T_AlterOwnerStmt:
			/* local only for now*/
			break;

		case T_DropOwnedStmt:
			error_unsupported_command(CreateCommandTag(parsetree));
			break;

		case T_AlterDefaultPrivilegesStmt:
			lock_type = BDR_LOCK_DDL;
			break;

		case T_SecLabelStmt:
			{
				SecLabelStmt *sstmt;
				sstmt = (SecLabelStmt *) parsetree;

				if (sstmt->provider == NULL ||
					strcmp(sstmt->provider, "bdr") == 0)
					break;
				error_unsupported_command(CreateCommandTag(parsetree));
				break;
			}
		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(parsetree));
			break;
	}

	/* now lock other nodes in the bdr flock against ddl */
	if (!bdr_skip_ddl_locking && !statement_affects_only_nonpermanent(parsetree))
		bdr_acquire_ddl_lock(lock_type);

done:
	if (nodeTag(parsetree) == T_TruncateStmt)
		bdr_start_truncate();

	if (next_ProcessUtility_hook)
		next_ProcessUtility_hook(parsetree, queryString, context, params,
								 dest, completionTag);
	else
		standard_ProcessUtility(parsetree, queryString, context, params,
								dest, completionTag);

	if (nodeTag(parsetree) == T_TruncateStmt)
		bdr_finish_truncate();
}

static void
bdr_ClientAuthentication_hook(Port *port, int status)
{
	if (MyProcPort->database_name != NULL
		&& strcmp(MyProcPort->database_name, BDR_SUPERVISOR_DBNAME) == 0)
	{

		/*
		 * No commands may be executed under the supervisor database.
		 *
		 * This check won't catch execution attempts by bgworkers, but as currently
		 * database_name isn't set for those. They'd better just know better.  It's
		 * relatively harmless to run things in the supervisor database anyway.
		 *
		 * Make it a warning because of #154. Tools like vacuumdb -a like to
		 * connect to all DBs.
		 */
		ereport(WARNING,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("The BDR extension reserves the database "
						BDR_SUPERVISOR_DBNAME" for its own use"),
				 errhint("Use a different database")));
	}

	if (next_ClientAuthentication_hook)
		next_ClientAuthentication_hook(port, status);
}


/* Module load */
void
init_bdr_commandfilter(void)
{
	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = bdr_commandfilter;

	next_ClientAuthentication_hook = ClientAuthentication_hook;
	ClientAuthentication_hook = bdr_ClientAuthentication_hook;
}
