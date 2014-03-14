/*-------------------------------------------------------------------------
 *
 * ruleutils.h
 *		Declarations for ruleutils.c
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/ruleutils.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RULEUTILS_H
#define RULEUTILS_H

#include "catalog/pg_trigger.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"


extern bool quote_all_identifiers;

extern void pg_get_ruledef_details(Datum ev_qual, Datum ev_action,
					   char **whereClause, List **actions);
extern char *pg_get_viewdef_internal(Oid viewoid);
extern char *pg_get_indexdef_string(Oid indexrelid);
extern char *pg_get_indexdef_columns(Oid indexrelid, bool pretty);
extern void pg_get_indexdef_detailed(Oid indexrelid,
						 char **index_am,
						 char **definition,
						 char **reloptions,
						 char **tablespace,
						 char **whereClause);
extern char *pg_get_trigger_whenclause(Form_pg_trigger trigrec,
						  Node *whenClause, bool pretty);


extern char *pg_get_constraintdef_string(Oid constraintId, bool fullCommand);
extern char *pg_get_viewstmt_definition(Query *viewParse);
extern char *deparse_expression(Node *expr, List *dpcontext,
				   bool forceprefix, bool showimplicit);
extern List *deparse_context_for(const char *aliasname, Oid relid);
extern List *deparse_context_for_planstate(Node *planstate, List *ancestors,
							  List *rtable, List *rtable_names);
extern List *select_rtable_names_for_explain(List *rtable,
								Bitmapset *rels_used);
extern const char *quote_identifier(const char *ident);
extern char *quote_qualified_identifier(const char *qualifier,
						   const char *ident);
extern char *generate_collation_name(Oid collid);

/* quote.c */
extern char *quote_literal_cstr(const char *rawstr);

#endif	/* RULEUTILS_H */
