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

/*
 * Support for keeping track of a command to deparse.
 *
 * When a command is run, we collect some information about it for later
 * deparsing; deparse_utility_command can later be used to obtain a usable
 * representation of it.
 */


typedef struct StashedCommand
{
	ObjectType	objtype;
	Oid			objectId;
	Node	   *parsetree;
} StashedCommand;

extern char *deparse_utility_command(StashedCommand *cmd);

#endif	/* DEPARSE_UTILITY_H */
