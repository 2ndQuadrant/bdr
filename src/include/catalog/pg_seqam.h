/*-------------------------------------------------------------------------
 *
 * pg_seqam.h
 *	  definition of the system "sequence access method" relation (pg_seqam)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_seqam.h
 *
 * NOTES
 *		the genbki.pl script reads this file and generates .bki
 *		information from the DATA() statements.
 *
 *		XXX do NOT break up DATA() statements into multiple lines!
 *			the scripts are not as smart as you might think...
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_SEQAM_H
#define PG_SEQAM_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_seqam definition.  cpp turns this into
 *		typedef struct FormData_pg_seqam
 * ----------------
 */
#define SeqAccessMethodRelationId	32

CATALOG(pg_seqam,32)
{
	NameData	seqamname;			/* access method name */
	regproc		seqamalloc;			/* get next allocation of range of values function */
	regproc		seqamsetval;		/* set value function */
	regproc		seqamoptions;		/* parse AM-specific parameters */
} FormData_pg_seqam;

/* ----------------
 *		Form_pg_seqam corresponds to a pointer to a tuple with
 *		the format of pg_seqam relation.
 * ----------------
 */
typedef FormData_pg_seqam *Form_pg_seqam;

/* ----------------
 *		compiler constants for pg_seqam
 * ----------------
 */
#define Natts_pg_seqam						4
#define Anum_pg_seqam_amname				1
#define Anum_pg_seqam_amalloc				2
#define Anum_pg_seqam_amsetval				3
#define Anum_pg_seqam_amoptions				4

/* ----------------
 *		initial contents of pg_seqam
 * ----------------
 */

DATA(insert OID = 2 (  local		sequence_local_alloc sequence_local_setval sequence_local_options));
DESCR("local sequence access method");
#define LOCAL_SEQAM_OID 2

#define DEFAULT_SEQAM	""

#endif   /* PG_SEQAM_H */
