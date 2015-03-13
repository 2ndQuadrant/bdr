/*-------------------------------------------------------------------------
 *
 * deparse_utility.h
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/deparse_utility.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DEPARSE_UTILITY_H
#define DEPARSE_UTILITY_H

#include "access/attnum.h"
#include "nodes/nodes.h"
#include "utils/aclchk.h"


/*
 * Support for keeping track of a command to deparse.
 *
 * When a command is run, we collect some information about it for later
 * deparsing; deparse_utility_command can later be used to obtain a usable
 * representation of it.
 */

typedef enum StashedCommandType
{
	SCT_Simple,
	SCT_AlterTable,
	SCT_Grant,
	SCT_AlterOpFamily
} StashedCommandType;

/*
 * For ALTER TABLE commands, we keep a list of the subcommands therein.
 */
typedef struct StashedATSubcmd
{
	AttrNumber		attnum;	/* affected column number */
	Oid				oid;	/* affected constraint, default value or index */
	Node		   *parsetree;
} StashedATSubcmd;

typedef struct StashedCommand
{
	StashedCommandType type;
	bool		in_extension;
	Node	   *parsetree;
	slist_node	link;

	union
	{
		struct SimpleCommand
		{
			ObjectAddress address;
			ObjectAddress secondaryObject;
		} simple;

		struct AlterTableCommand
		{
			Oid		objectId;
			Oid		classId;
			List   *subcmds;
		} alterTable;

		struct GrantCommand
		{
			InternalGrant *istmt;
			const char *type;
		} grant;

		struct AlterOpFamily
		{
			Oid		opfamOid;
			List   *operators;
			List   *procedures;
		} opfam;
	} d;
} StashedCommand;

extern char *deparse_utility_command(StashedCommand *cmd);

#endif	/* DEPARSE_UTILITY_H */
