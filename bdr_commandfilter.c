/* -------------------------------------------------------------------------
 *
 * bdr_commandfilter.c
 *		prevent execution of utility commands not yet or never supported
 *
 *
 * Copyright (C) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/bdr/bdr_commandfilter.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "bdr.h"
#include "bdr_locks.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "access/heapam.h"
#ifdef BUILDING_BDR
#include "access/seqam.h"
#endif

#include "catalog/namespace.h"

#include "commands/dbcommands.h"
#include "commands/event_trigger.h"
#include "commands/extension.h"
#include "commands/tablecmds.h"

#include "parser/parse_utilcmd.h"

#include "storage/standby.h"

#include "tcop/utility.h"

#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
* bdr_commandfilter.c: a ProcessUtility_hook to prevent a cluster from running
* commands that BDR does not yet support.
*/

static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;

/* GUCs */
bool bdr_permit_unsafe_commands = false;

bool bdr_always_allow_ddl = false;

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

static void
filter_CreateStmt(Node *parsetree,
				  char *completionTag)
{
	CreateStmt *stmt;
	ListCell   *cell;

	stmt = (CreateStmt *) parsetree;

	if (stmt->ofTypename != NULL)
		error_unsupported_command("CREATE TABLE ... OF TYPE");

	foreach(cell, stmt->tableElts)
	{
		Node	   *element = lfirst(cell);

		if (nodeTag(element) == T_Constraint)
		{
			Constraint *con = (Constraint *) element;

			if (con->contype == CONSTR_EXCLUSION &&
				stmt->relation->relpersistence != RELPERSISTENCE_TEMP)
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
					  const char *queryString)
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

	lockmode = AlterTableGetLockLevel(astmt->cmds);
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
										 AlterTableGetLockLevel(astmt->cmds),
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
										 AlterTableGetLockLevel(astmt->cmds),
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

				case AT_SetRelOptions:	/* SET (...) */
				case AT_ResetRelOptions:		/* RESET (...) */
				case AT_ReplaceRelOptions:		/* replace reloption list */
				case AT_ReplicaIdentity:
				case AT_ChangeOwner:
				case AT_SetStorage:
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
										 AlterTableGetLockLevel(astmt->cmds),
												   astmt->missing_ok);
					}
					break;

				case AT_ValidateConstraint: /* VALIDATE CONSTRAINT */
					break;

				case AT_AlterConstraint:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ALTER CONSTRAINT",
										   AlterTableGetLockLevel(astmt->cmds),
										   astmt->missing_ok);
					break;

				case AT_AddIndexConstraint:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ADD CONSTRAINT USING INDEX",
										   AlterTableGetLockLevel(astmt->cmds),
										   astmt->missing_ok);
					break;

				case AT_AlterColumnType:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ALTER COLUMN TYPE",
										   AlterTableGetLockLevel(astmt->cmds),
										   astmt->missing_ok);
					break;

				case AT_AlterColumnGenericOptions:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ALTER COLUMN OPTIONS",
										   AlterTableGetLockLevel(astmt->cmds),
										   astmt->missing_ok);
					break;

				case AT_AddOids:
				case AT_DropOids:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... SET WITH[OUT] OIDS",
										   AlterTableGetLockLevel(astmt->cmds),
										   astmt->missing_ok);
					break;

				case AT_EnableTrig:
				case AT_EnableAlwaysTrig:
				case AT_EnableReplicaTrig:
				case AT_DisableTrig:
				case AT_EnableTrigAll:
				case AT_DisableTrigAll:
				case AT_EnableTrigUser:
				case AT_DisableTrigUser:
					break;

				case AT_EnableRule:
				case AT_EnableAlwaysRule:
				case AT_EnableReplicaRule:
				case AT_DisableRule:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ENABLE|DISABLE [ALWAYS|REPLICA] RULE",
										   AlterTableGetLockLevel(astmt->cmds),
										   astmt->missing_ok);
					break;

				case AT_AddInherit:
				case AT_DropInherit:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ADD|DROP INHERIT",
										   AlterTableGetLockLevel(astmt->cmds),
										   astmt->missing_ok);
					break;

				case AT_AddOf:
				case AT_DropOf:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... [NOT] OF",
										   AlterTableGetLockLevel(astmt->cmds),
										   astmt->missing_ok);
					break;

				case AT_SetStatistics:
					break;
				case AT_SetOptions:
				case AT_ResetOptions:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... ALTER COLUMN SET STATISTICS|(...)",
										   AlterTableGetLockLevel(astmt->cmds),
										   astmt->missing_ok);
					break;

				case AT_GenericOptions:
					error_on_persistent_rv(astmt->relation,
										   "ALTER TABLE ... SET (...)",
										   AlterTableGetLockLevel(astmt->cmds),
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
							   AlterTableGetLockLevel(astmt->cmds),
							   astmt->missing_ok);
}

static void
filter_CreateSeqStmt(Node *parsetree)
{
#ifdef BUILDING_BDR
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
#endif
}

static void
filter_AlterSeqStmt(Node *parsetree)
{
#ifdef BUILDING_BDR
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
#endif
}

static void
filter_CreateTableAs(Node *parsetree)
{
	CreateTableAsStmt *stmt;
	stmt = (CreateTableAsStmt *) parsetree;

	if (stmt->into->rel->relpersistence != RELPERSISTENCE_TEMP)
		error_unsupported_command(CreateCommandTag(parsetree));
}

static void
bdr_commandfilter(Node *parsetree,
				  const char *queryString,
				  ProcessUtilityContext context,
				  ParamListInfo params,
				  DestReceiver *dest,
				  char *completionTag)
{
	/* don't filter in single user mode */
	if (!IsUnderPostmaster)
		goto done;

	/* don't filter if explicitly told so */
	if (bdr_always_allow_ddl || bdr_permit_unsafe_commands)
		goto done;

	/* extension contents aren't individually replicated */
	if (creating_extension)
		goto done;

	/* don't perform filtering while replaying */
	if (replication_origin_id != InvalidRepNodeId)
		goto done;

	/* commands we skip (for now) */
	switch (nodeTag(parsetree))
	{
		case T_TransactionStmt:
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
		case T_CreatedbStmt:
		case T_AlterDatabaseStmt:
		case T_AlterDatabaseSetStmt:
		case T_DropdbStmt:
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

#ifdef BUILDING_UDR
	if (!in_bdr_replicate_ddl_command && bdr_is_bdr_activated_db())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("DDL commands are not allowed when UDR is active unless bdr.permit_unsafe_ddl_commands is true")));
#endif /*BUILDING_UDR*/

	/* statements handled directly in standard_ProcessUtility */
	switch (nodeTag(parsetree))
	{
		case T_DropStmt:
			{
				DropStmt   *stmt = (DropStmt *) parsetree;

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

				if (EventTriggerSupportsObjectType(stmt->objectType))
					break;
				else
					goto done;
			}

		default:
			break;
	}

	/*
	 * Don't filter if this database isn't using bdr. Check after the above
	 * fasts tests as this can be comparatively expensive. This also requires
	 * to be in a transaction and thus isn't admissible for transaction
	 * control commands.
	 */
	if (!bdr_is_bdr_activated_db())
		goto done;

	/* all commands handled by ProcessUtilitySlow() */
	switch (nodeTag(parsetree))
	{
		case T_CreateSchemaStmt:
			break;

		case T_CreateStmt:
			filter_CreateStmt(parsetree, completionTag);
			break;

		case T_CreateForeignTableStmt:
			break;

		case T_AlterTableStmt:
			filter_AlterTableStmt(parsetree, completionTag, queryString);
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
				break;
			}
		case T_CreateExtensionStmt:
			break;

		case T_AlterExtensionStmt:
			/* XXX: we could support some of these */
			error_unsupported_command(completionTag);
			break;

		case T_AlterExtensionContentsStmt:
			error_unsupported_command(completionTag);
			break;

		case T_CreateFdwStmt:
		case T_AlterFdwStmt:
		case T_CreateForeignServerStmt:
		case T_AlterForeignServerStmt:
		case T_CreateUserMappingStmt:
		case T_AlterUserMappingStmt:
		case T_DropUserMappingStmt:
			/* XXX: we should probably support all of these at some point */
			error_unsupported_command(completionTag);
			break;

		case T_CompositeTypeStmt:	/* CREATE TYPE (composite) */
		case T_CreateEnumStmt:		/* CREATE TYPE AS ENUM */
		case T_CreateRangeStmt:		/* CREATE TYPE AS RANGE */
			break;

		case T_AlterEnumStmt:
		case T_ViewStmt:	/* CREATE VIEW */
		case T_CreateFunctionStmt:	/* CREATE FUNCTION */
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
			error_unsupported_command(completionTag);
			break;

		case T_CreateTrigStmt:
			break;

		case T_CreatePLangStmt:
			error_unsupported_command(completionTag);
			break;

		case T_CreateDomainStmt:
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
	if (!bdr_skip_ddl_locking)
		bdr_acquire_ddl_lock();

done:
	if (next_ProcessUtility_hook)
		next_ProcessUtility_hook(parsetree, queryString, context, params,
								 dest, completionTag);
	else
		standard_ProcessUtility(parsetree, queryString, context, params,
								dest, completionTag);
}

void
bdr_commandfilter_always_allow_ddl(bool always_allow)
{
	Assert(IsUnderPostmaster);
	bdr_always_allow_ddl = always_allow;
}

/* Module load */
void
init_bdr_commandfilter(void)
{
	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = bdr_commandfilter;
}
