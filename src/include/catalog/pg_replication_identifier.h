/*-------------------------------------------------------------------------
 *
 * pg_replication_identifier.h
 *	  Persistent Replication Node Identifiers
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
#define ReplicationIdentifierRelationId 6000

CATALOG(pg_replication_identifier,6000) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	/*
	 * locally known identifier that gets included into wal.
	 *
	 * This should never leave the system.
	 *
	 * Needs to fit into a uint16, so we don't waste too much space in WAL
	 * records. For this reason we don't use a normal Oid column here, since
	 * we need to handle allocation of new values manually.
	 */
	Oid		riident;

	/*
	 * Variable-length fields start here, but we allow direct access to
	 * riname.
	 */

	/* external, free-format, identifier */
	text	riname;
#ifdef CATALOG_VARLEN		/* further variable-length fields */
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

#define Natts_pg_replication_identifier		2
#define Anum_pg_replication_riident			1
#define Anum_pg_replication_riname			2

/* ----------------
 *		pg_replication_identifier has no initial contents
 * ----------------
 */

#endif   /* PG_REPLICTION_IDENTIFIER_H */
