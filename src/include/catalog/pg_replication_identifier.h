/*-------------------------------------------------------------------------
 *
 * pg_replication_identifier.h
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_replication_identifier.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_REPLICATION_IDENTIFIER_H
#define PG_REPLICATION_IDENTIFIER_H

#include "catalog/genbki.h"
#include "access/xlogdefs.h"

/* ----------------
 *		pg_replication_identifier.  cpp turns this into
 *		typedef struct FormData_pg_replication_identifier
 * ----------------
 */
#define ReplicationIdentifierRelationId 3458

CATALOG(pg_replication_identifier,3458) BKI_WITHOUT_OIDS
{
	/*
	 * locally known identifier that gets included into wal.
	 *
	 * This should never leave the system.
	 *
	 * Needs to fit into a uint16, so we don't waste too much space. For this
	 * reason we don't use a normal Oid column here, since we need to handle
	 * allocation of new values manually.
	 */
	Oid		riident;

	/* ----
	 * remote system identifier, including tli, separated by a -.
	 *
	 * They are packed together for two reasons:
	 * a) we can't represent sysids as uint64 because there's no such type on
	 * 	  sql level, so we need a fixed width string anyway. And a name already
	 * 	  has enough space for that.
	 * b) syscaches can only have 4 keys, and were already at that with
	 *    combined keys
	 * ----
	 */
	NameData riremotesysid;

	/* local database */
	Oid		rilocaldb;

	/* remote database */
	Oid		riremotedb;

	/* optional name, zero */
	NameData riname;
#ifdef CATALOG_VARLEN		/* variable-length fields start here */
#endif
} FormData_pg_replication_identifier;

/* ----------------
 *		Form_pg_extension corresponds to a pointer to a tuple with
 *		the format of pg_extension relation.
 * ----------------
 */
typedef FormData_pg_replication_identifier *Form_pg_replication_identifier;

/* ----------------
 *		compiler constants for pg_replication_identifier
 * ----------------
 */

#define Natts_pg_replication_identifier		5
#define Anum_pg_replication_riident			1
#define Anum_pg_replication_riremotesysid	2
#define Anum_pg_replication_rilocaldb		3
#define Anum_pg_replication_riremotedb		4
#define Anum_pg_replication_riname			5

/* ----------------
 *		pg_replication_identifier has no initial contents
 * ----------------
 */

#endif   /* PG_REPLICTION_IDENTIFIER_H */
