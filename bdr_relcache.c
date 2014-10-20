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

#include "commands/seclabel.h"

#include "utils/catcache.h"
#include "utils/inval.h"

static HTAB *BDRRelcacheHash = NULL;

static void
BDRRelcacheHashInvalidateEntry(BDRRelation *entry)
{
	int i;

	if (entry->conflict_handlers)
		pfree(entry->conflict_handlers);

	if (entry->num_replication_sets > 0)
	{
		for (i = 0; i < entry->num_replication_sets; i++)
			pfree(entry->replication_sets[i]);

		pfree(entry->replication_sets);
	}
}

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
			BDRRelcacheHashInvalidateEntry(entry);

			if (hash_search(BDRRelcacheHash, &entry->reloid,
							HASH_REMOVE, NULL) == NULL)
				elog(ERROR, "hash table corrupted");
		}
	}
	else
	{
		if ((entry = hash_search(BDRRelcacheHash, &relid,
								 HASH_FIND, NULL)) != NULL)
		{
			BDRRelcacheHashInvalidateEntry(entry);

			hash_search(BDRRelcacheHash, &relid,
						HASH_REMOVE, NULL);
		}
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

#include "utils/jsonapi.h"
#include "utils/json.h"
#include "utils/jsonb.h"

void
bdr_parse_relation_options(const char *label, BDRRelation *rel)
{
	JsonbIterator *it;
	JsonbValue	v;
	int			r;
	bool		parsing_sets = false;
	int			level = 0;
	Jsonb	*data = NULL;

	if (label == NULL)
		return;

	data = DatumGetJsonb(
		DirectFunctionCall1(jsonb_in, CStringGetDatum(label)));

	if (!JB_ROOT_IS_OBJECT(data))
		elog(ERROR, "root needs to be an object");

	it = JsonbIteratorInit(&data->root);
	while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		if (level == 0 && r != WJB_BEGIN_OBJECT)
			elog(ERROR, "root element needs to be an object");
		else if (level == 0 && it->nElems > 1)
			elog(ERROR, "only 'sets' allowed on root level");
		else if (level == 1 && r == WJB_KEY)
		{
			if (strncmp(v.val.string.val, "sets", v.val.string.len) != 0)
				elog(ERROR, "unexpected key: %s",
					 pnstrdup(v.val.string.val, v.val.string.len));
			parsing_sets = true;
		}
		else if (r == WJB_BEGIN_ARRAY || r == WJB_BEGIN_OBJECT)
		{
			if (parsing_sets && rel != NULL)
			{
				rel->replication_sets =
					MemoryContextAlloc(CacheMemoryContext,
									   sizeof(char *) * it->nElems);
			}
			level++;
		}
		else if (r == WJB_END_ARRAY || r == WJB_END_OBJECT)
		{
			level--;
			parsing_sets = false;
		}
		else if (parsing_sets)
		{
			char *setname;

			if (r != WJB_ELEM)
				elog(ERROR, "unexpected element type %u", r);
			if (level != 2)
				elog(ERROR, "unexpected level for set %d", level);

			if (rel != NULL)
			{
				MemoryContext oldcontext;

				oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
				setname = pnstrdup(v.val.string.val, v.val.string.len);
				rel->replication_sets[rel->num_replication_sets++] = setname;
				MemoryContextSwitchTo(oldcontext);
			}
		}
		else
			elog(ERROR, "unexpected content: %u at level %d", r, level);
	}

	if (rel != NULL && rel->num_replication_sets > 0)
	{
			qsort(rel->replication_sets, rel->num_replication_sets,
				  sizeof(char *), pg_qsort_strcmp);
	}

}

BDRRelation *
bdr_heap_open(Oid reloid, LOCKMODE lockmode)
{
	BDRRelation *entry;
	bool		found;
	Relation	rel;
	ObjectAddress object;
	const char *label;

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

	object.classId = RelationRelationId;
	object.objectId = reloid;
	object.objectSubId = 0;

	label = GetSecurityLabel(&object, "bdr");
	bdr_parse_relation_options(label, entry);

	return entry;
}

void
bdr_heap_close(BDRRelation * rel, LOCKMODE lockmode)
{
	heap_close(rel->rel, lockmode);
	rel->rel = NULL;
}
