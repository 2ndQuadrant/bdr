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

#include "access/heapam.h"
#include "bdr.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "utils/guc.h"
#include "utils/rel.h"

/*
* bdr_commandfilter.c: a ProcessUtility_hook to prevent a cluster from running
* commands that BDR does not yet support.
*/

static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;

static bool bdr_permit_unsafe_commands;

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
					   int severity,
					   bool missing_ok)
{
	bool		needswal;
	Relation	rel;

	if (rv == NULL)
		ereport(severity,
				(errmsg("Unqualified command %s is unsafe with BDR active.",
						cmdtag)));

	rel = heap_openrv_extended(rv, lockmode, missing_ok);

	if (rel != NULL)
	{
		needswal = RelationNeedsWAL(rel);
		heap_close(rel, lockmode);
		if (needswal)
			ereport(severity,
				 (errmsg("%s may only affect UNLOGGED or TEMPORARY tables " \
						 "when BDR is active; %s is a regular table",
						 cmdtag, rv->relname)));
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
	int			severity = ERROR;
	ListCell   *cell;
	AlterTableStmt *alterTableStatement;
	CreateStmt *createStatement;
	bool hasInvalid;
	Constraint *con;

	ereport(DEBUG4,
		 (errmsg_internal("bdr_commandfilter ProcessUtility_hook invoked")));
	if (bdr_permit_unsafe_commands)
		severity = WARNING;


	switch (nodeTag(parsetree))
	{
		case T_SecLabelStmt:	/* XXX what about this? */
			error_on_persistent_rv(((ClusterStmt *) parsetree)->relation,
							 "SECURITY LABEL", AccessExclusiveLock, severity,
								   false);
			break;

		case T_CreateStmt:
			createStatement = (CreateStmt *) parsetree;

			foreach(cell, createStatement->tableElts)
			{
				Node *element = lfirst(cell);

				if (nodeTag(element) == T_Constraint)
				{
					con = (Constraint *) element;
					if (con->contype == CONSTR_EXCLUSION &&
						createStatement->relation->relpersistence != RELPERSISTENCE_TEMP)
						ereport(severity,
								(errmsg("EXCLUDE constraints are unsafe with BDR active")));
				}
			}

			break;

			/*
			 * XXX allow ALTER TABLE statements when stabilized; still forbid *
			 * ALTER TABLE RENAME as well as ALTER TYPE (which seems to be
			 * handled by ALTER TYPE, too)
			 */
		case T_AlterTableStmt:
			alterTableStatement = (AlterTableStmt *) parsetree;
			hasInvalid = false;

			foreach(cell, alterTableStatement->cmds)
			{
				AlterTableCmd *stmt = (AlterTableCmd *) lfirst(cell);

				switch (stmt->subtype)
				{
					/*
					 * allowed for now:
					 */
					case AT_AddColumn: /* add column */

					case AT_ColumnDefault: /* ALTER COLUMN DEFAULT */

					case AT_ClusterOn: /* CLUSTER ON */
					case AT_DropCluster: /* SET WITHOUT CLUSTER */

					case AT_SetRelOptions: /* SET (...) */
					case AT_ResetRelOptions: /* RESET (...) */
					case AT_ReplaceRelOptions: /* replace reloption list */
						break;

					case AT_AddConstraint:
						if (IsA(stmt->def, Constraint))
						{
							con = (Constraint *) stmt->def;

							if (con->contype == CONSTR_EXCLUSION)
								error_on_persistent_rv(alterTableStatement->relation,
													   "ALTER TABLE ... ADD CONSTRAINT ... EXCLUDE",
													   AccessExclusiveLock, severity,
													   alterTableStatement->missing_ok);
						}
						break;

					default:
						hasInvalid = true;
						break;
				}
			}

			if (hasInvalid)
				error_on_persistent_rv(alterTableStatement->relation,
									   "ALTER TABLE", AccessExclusiveLock,
									   severity, alterTableStatement->missing_ok);
			break;

		case T_AlterEnumStmt:
			ereport(severity,
			 (errmsg("ALTER TYPE ... ADD VALUE is unsafe with BDR active")));
			break;

		default:
			break;
	}

	if (next_ProcessUtility_hook)
	{
		ereport(DEBUG4,
				(errmsg_internal("bdr_commandfilter ProcessUtility_hook " \
								 "handing off to next hook ")));
		(*next_ProcessUtility_hook) (parsetree, queryString, context, params,
									 dest, completionTag);
	}
	else
	{
		ereport(DEBUG4,
				(errmsg_internal("bdr_commandfilter ProcessUtility_hook " \
								 "invoking standard_ProcessUtility")));
		standard_ProcessUtility(parsetree, queryString, context, params,
								dest, completionTag);
	}
}

/* Module load */
void
init_bdr_commandfilter(void)
{
	ereport(DEBUG4, (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
		errmsg_internal("bdr_commandfilter ProcessUtility_hook installed")));
	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = bdr_commandfilter;

	DefineCustomBoolVariable("bdr.permit_unsafe_ddl_commands",
							 "Allow commands that might cause data or " \
							 "replication problems under BDR to run",
							 NULL,
							 &bdr_permit_unsafe_commands,
							 false, PGC_SUSET,
							 0,
							 NULL, NULL, NULL);
}
