/*-------------------------------------------------------------------------
 *
 * committsdesc.c
 *    rmgr descriptor routines for access/transam/committs.c
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/access/rmgrdesc/committsdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/committs.h"
#include "utils/timestamp.h"


void
committs_desc(StringInfo buf, uint8 xl_info, char *rec)
{
	uint8		info = xl_info & ~XLR_INFO_MASK;

	if (info == COMMITTS_ZEROPAGE)
	{
		int			pageno;

		memcpy(&pageno, rec, sizeof(int));
		appendStringInfo(buf, "zeropage: %d", pageno);
	}
	else if (info == COMMITTS_TRUNCATE)
	{
		int			pageno;

		memcpy(&pageno, rec, sizeof(int));
		appendStringInfo(buf, "truncate before: %d", pageno);
	}
	else if (info == COMMITTS_SETTS)
	{
		xl_committs_set *xlrec = (xl_committs_set *) rec;
		int		i;

		appendStringInfo(buf, "set committs %s for: %u",
						 timestamptz_to_str(xlrec->timestamp),
						 xlrec->mainxid);
		for (i = 0; i < xlrec->nsubxids; i++)
			appendStringInfo(buf, ", %u", xlrec->subxids[i]);
	}
	else
		appendStringInfo(buf, "UNKNOWN");
}
