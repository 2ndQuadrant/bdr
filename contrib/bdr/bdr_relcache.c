/* -------------------------------------------------------------------------
 *
 * bdr_relcache.c
 *		BDR relation caching
 *
 * Caching relation specific information
 *
 * Copyright (C) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/bdr/bdr_relcache.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "bdr.h"

#include "access/heapam.h"
#include "access/xact.h"

#include "utils/catcache.h"
#include "utils/inval.h"

static HTAB *BDRRelcacheHash = NULL;

static void
BDRRelcacheHashInvalidateCallback(Datum arg, Oid relid)
{
	HASH_SEQ_STATUS status;
	BDRRelation *entry;

	/* callback only gets registered after creating the hash */
	Assert(BDRRelcacheHash != NULL);

	/*
	 * If relid is InvalidOid, signalling a complete reset, we have to remove
	 * all entries, otherwise just remove the specific relation's entry.
	 */
	if (relid == InvalidOid)
	{
		hash_seq_init(&status, BDRRelcacheHash);

		while ((entry = (BDRRelation *) hash_seq_search(&status)) != NULL)
		{
			if (entry->conflict_handlers)
				pfree(entry->conflict_handlers);

			if (hash_search(BDRRelcacheHash, (void *) &entry->reloid,
							HASH_REMOVE, NULL) == NULL)
				elog(ERROR, "hash table corrupted");
		}
	}
	else
	{
		entry = hash_search(BDRRelcacheHash, (void *) &relid,
							HASH_REMOVE, NULL);

		if (entry && entry->conflict_handlers)
			pfree(entry->conflict_handlers);
	}
}

static void
bdr_initialize_cache()
{
	HASHCTL		ctl;

	/* Make sure we've initialized CacheMemoryContext. */
	if (CacheMemoryContext == NULL)
		CreateCacheMemoryContext();

	/* Initialize the hash table. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(BDRRelation);
	ctl.hash = tag_hash;
	ctl.hcxt = CacheMemoryContext;

	BDRRelcacheHash = hash_create("BDR relation cache", 128, &ctl,
								  HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(BDRRelcacheHashInvalidateCallback,
								  (Datum) 0);
}

BDRRelation *
bdr_heap_open(Oid reloid, LOCKMODE lockmode)
{
	BDRRelation *entry;
	bool		found;
	Relation	rel;

	rel = heap_open(reloid, lockmode);

	if (BDRRelcacheHash == NULL)
		bdr_initialize_cache();

	/*
	 * HASH_ENTER returns the existing entry if present or creates a new one.
	 */
	entry = hash_search(BDRRelcacheHash, (void *) &reloid,
						HASH_ENTER, &found);

	if (found)
	{
		entry->rel = rel;
		return entry;
	}

	/* zero out data part of the entry */
	memset(((char *) entry) + sizeof(Oid), 0,
		   sizeof(BDRRelation) - sizeof(Oid));

	entry->reloid = reloid;
	entry->rel = rel;

	return entry;
}

void
bdr_heap_close(BDRRelation * rel, LOCKMODE lockmode)
{
	heap_close(rel->rel, lockmode);
	rel->rel = NULL;
}
