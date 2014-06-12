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
#include "utils/aclchk_internal.h"


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
	SCT_Grant
} StashedCommandType;

/*
 * For ALTER TABLE commands, we keep a list of the subcommands therein.
 */
typedef struct StashedATSubcmd
{
	ObjectAddress	address; /* affected column, constraint, index, ... */
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
		/* most commands */
		struct
		{
			ObjectAddress address;
			ObjectAddress secondaryObject;
		} simple;

		/* ALTER TABLE, and internal uses thereof */
		struct
		{
			Oid		objectId;
			Oid		classId;
			List   *subcmds;
		} alterTable;

		/* GRANT / REVOKE */
		struct
		{
			InternalGrant *istmt;
			const char *type;
		} grant;
	} d;
} StashedCommand;

extern char *deparse_utility_command(StashedCommand *cmd);

#endif	/* DEPARSE_UTILITY_H */
