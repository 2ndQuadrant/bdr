/*
 * brin_page.h
 *		Prototypes and definitions for BRIN page layouts
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/include/access/brin_page.h
 *
 * NOTES
 *
 * These structs should really be private to specific BRIN files, but it's
 * useful to have them here so that they can be used by pageinspect and similar
 * tools.
 */
#ifndef BRIN_PAGE_H
#define BRIN_PAGE_H

#include "storage/block.h"
#include "storage/itemptr.h"

/* special space on all BRIN pages stores a "type" identifier */
#define		BRIN_PAGETYPE_META			0xF091
#define		BRIN_PAGETYPE_REVMAP		0xF092
#define		BRIN_PAGETYPE_REGULAR		0xF093

#define BRIN_PAGE_TYPE(page)	\
	(((BrinSpecialSpace *) PageGetSpecialPointer(page))->type)
#define BRIN_IS_REVMAP_PAGE(page) (BRIN_PAGE_TYPE(page) == BRIN_PAGETYPE_REVMAP)
#define BRIN_IS_REGULAR_PAGE(page) (BRIN_PAGE_TYPE(page) == BRIN_PAGETYPE_REGULAR)

/* flags for BrinSpecialSpace */
#define		BRIN_EVACUATE_PAGE			(1 << 0)

typedef struct BrinSpecialSpace
{
	uint16		flags;
	uint16		type;
} BrinSpecialSpace;

/* Metapage definitions */
typedef struct BrinMetaPageData
{
	uint32		brinMagic;
	uint32		brinVersion;
	BlockNumber pagesPerRange;
	BlockNumber lastRevmapPage;
} BrinMetaPageData;

#define BRIN_CURRENT_VERSION		1
#define BRIN_META_MAGIC			0xA8109CFA

#define BRIN_METAPAGE_BLKNO		0

/* Definitions for revmap pages */
typedef struct RevmapContents
{
	ItemPointerData rm_tids[1]; /* really REVMAP_PAGE_MAXITEMS */
} RevmapContents;

#define REVMAP_CONTENT_SIZE \
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - \
	 offsetof(RevmapContents, rm_tids) - \
	 MAXALIGN(sizeof(BrinSpecialSpace)))
/* max num of items in the array */
#define REVMAP_PAGE_MAXITEMS \
	(REVMAP_CONTENT_SIZE / sizeof(ItemPointerData))

#endif   /* BRIN_PAGE_H */
