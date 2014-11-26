/*-------------------------------------------------------------------------
 *
 * event_trigger.c
 *	  PostgreSQL EVENT TRIGGER support code.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/commands/event_trigger.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/event_trigger.h"
#include "commands/trigger.h"
#include "funcapi.h"
#include "parser/parse_func.h"
#include "pgstat.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "tcop/deparse_utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/evtcache.h"
#include "utils/fmgroids.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tqual.h"
#include "utils/syscache.h"
#include "tcop/utility.h"


typedef struct EventTriggerQueryState
{
	slist_head	SQLDropList;
	bool		in_sql_drop;
	MemoryContext cxt;
	StashedCommand *curcmd;
	List	   *stash;		/* list of StashedCommand; see deparse_utility.h */
	bool		in_extension;
	struct EventTriggerQueryState *previous;
} EventTriggerQueryState;

static EventTriggerQueryState *currentEventTriggerState = NULL;

typedef struct
{
	const char *obtypename;
	bool		supported;
} event_trigger_support_data;

typedef enum
{
	EVENT_TRIGGER_COMMAND_TAG_OK,
	EVENT_TRIGGER_COMMAND_TAG_NOT_SUPPORTED,
	EVENT_TRIGGER_COMMAND_TAG_NOT_RECOGNIZED
} event_trigger_command_tag_check_result;

/* XXX merge this with ObjectTypeMap? */
static event_trigger_support_data event_trigger_support[] = {
	{"AGGREGATE", true},
	{"CAST", true},
	{"CONSTRAINT", true},
	{"COLLATION", true},
	{"CONVERSION", true},
	{"DATABASE", false},
	{"DOMAIN", true},
	{"EXTENSION", true},
	{"EVENT TRIGGER", false},
	{"FOREIGN DATA WRAPPER", true},
	{"FOREIGN TABLE", true},
	{"FUNCTION", true},
	{"INDEX", true},
	{"LANGUAGE", true},
	{"MATERIALIZED VIEW", true},
	{"OPERATOR", true},
	{"OPERATOR CLASS", true},
	{"OPERATOR FAMILY", true},
	{"ROLE", false},
	{"RULE", true},
	{"SCHEMA", true},
	{"SEQUENCE", true},
	{"SERVER", true},
	{"TABLE", true},
	{"TABLESPACE", false},
	{"TRIGGER", true},
	{"TEXT SEARCH CONFIGURATION", true},
	{"TEXT SEARCH DICTIONARY", true},
	{"TEXT SEARCH PARSER", true},
	{"TEXT SEARCH TEMPLATE", true},
	{"TYPE", true},
	{"USER MAPPING", true},
	{"VIEW", true},
	{NULL, false}
};

/* Support for dropped objects */
typedef struct SQLDropObject
{
	ObjectAddress address;
	const char *schemaname;
	const char *objname;
	const char *objidentity;
	const char *objecttype;
	List	   *addrnames;
	List	   *addrargs;
	ObjectAddress dependee;
	bool		original;
	bool		normal;
	slist_node	next;
} SQLDropObject;

static void AlterEventTriggerOwner_internal(Relation rel,
								HeapTuple tup,
								Oid newOwnerId);
static event_trigger_command_tag_check_result check_ddl_tag(const char *tag);
static void error_duplicate_filter_variable(const char *defname);
static Datum filter_list_to_array(List *filterlist);
static Oid insert_event_trigger_tuple(char *trigname, char *eventname,
						   Oid evtOwner, Oid funcoid, List *tags);
static void validate_ddl_tags(const char *filtervar, List *taglist);
static void EventTriggerInvoke(List *fn_oid_list, EventTriggerData *trigdata);

/*
 * Create an event trigger.
 */
Oid
CreateEventTrigger(CreateEventTrigStmt *stmt)
{
	HeapTuple	tuple;
	Oid			funcoid;
	Oid			funcrettype;
	Oid			evtowner = GetUserId();
	ListCell   *lc;
	List	   *tags = NULL;

	/*
	 * It would be nice to allow database owners or even regular users to do
	 * this, but there are obvious privilege escalation risks which would have
	 * to somehow be plugged first.
	 */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to create event trigger \"%s\"",
						stmt->trigname),
				 errhint("Must be superuser to create an event trigger.")));

	/* Validate event name. */
	if (strcmp(stmt->eventname, "ddl_command_start") != 0 &&
		strcmp(stmt->eventname, "ddl_command_end") != 0 &&
		strcmp(stmt->eventname, "sql_drop") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("unrecognized event name \"%s\"",
						stmt->eventname)));

	/* Validate filter conditions. */
	foreach(lc, stmt->whenclause)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "tag") == 0)
		{
			if (tags != NULL)
				error_duplicate_filter_variable(def->defname);
			tags = (List *) def->arg;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
			   errmsg("unrecognized filter variable \"%s\"", def->defname)));
	}

	/* Validate tag list, if any. */
	if ((strcmp(stmt->eventname, "ddl_command_start") == 0 ||
		 strcmp(stmt->eventname, "ddl_command_end") == 0 ||
		 strcmp(stmt->eventname, "sql_drop") == 0)
		&& tags != NULL)
		validate_ddl_tags("tag", tags);

	/*
	 * Give user a nice error message if an event trigger of the same name
	 * already exists.
	 */
	tuple = SearchSysCache1(EVENTTRIGGERNAME, CStringGetDatum(stmt->trigname));
	if (HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("event trigger \"%s\" already exists",
						stmt->trigname)));

	/* Find and validate the trigger function. */
	funcoid = LookupFuncName(stmt->funcname, 0, NULL, false);
	funcrettype = get_func_rettype(funcoid);
	if (funcrettype != EVTTRIGGEROID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("function \"%s\" must return type \"event_trigger\"",
						NameListToString(stmt->funcname))));

	/* Insert catalog entries. */
	return insert_event_trigger_tuple(stmt->trigname, stmt->eventname,
									  evtowner, funcoid, tags);
}

/*
 * Validate DDL command tags.
 */
static void
validate_ddl_tags(const char *filtervar, List *taglist)
{
	ListCell   *lc;

	foreach(lc, taglist)
	{
		const char *tag = strVal(lfirst(lc));
		event_trigger_command_tag_check_result result;

		result = check_ddl_tag(tag);
		if (result == EVENT_TRIGGER_COMMAND_TAG_NOT_RECOGNIZED)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("filter value \"%s\" not recognized for filter variable \"%s\"",
							tag, filtervar)));
		if (result == EVENT_TRIGGER_COMMAND_TAG_NOT_SUPPORTED)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			/* translator: %s represents an SQL statement name */
					 errmsg("event triggers are not supported for %s",
							tag)));
	}
}

static event_trigger_command_tag_check_result
check_ddl_tag(const char *tag)
{
	const char *obtypename;
	event_trigger_support_data *etsd;

	/*
	 * Handle some idiosyncratic special cases.
	 */
	if (pg_strcasecmp(tag, "CREATE TABLE AS") == 0 ||
		pg_strcasecmp(tag, "SELECT INTO") == 0 ||
		pg_strcasecmp(tag, "REFRESH MATERIALIZED VIEW") == 0 ||
		pg_strcasecmp(tag, "ALTER DEFAULT PRIVILEGES") == 0 ||
		pg_strcasecmp(tag, "ALTER LARGE OBJECT") == 0 ||
		pg_strcasecmp(tag, "COMMENT") == 0 ||
		pg_strcasecmp(tag, "GRANT") == 0 ||
		pg_strcasecmp(tag, "REVOKE") == 0 ||
		pg_strcasecmp(tag, "DROP OWNED") == 0 ||
		pg_strcasecmp(tag, "SECURITY LABEL") == 0)
		return EVENT_TRIGGER_COMMAND_TAG_OK;

	/*
	 * Otherwise, command should be CREATE, ALTER, or DROP.
	 */
	if (pg_strncasecmp(tag, "CREATE ", 7) == 0)
		obtypename = tag + 7;
	else if (pg_strncasecmp(tag, "ALTER ", 6) == 0)
		obtypename = tag + 6;
	else if (pg_strncasecmp(tag, "DROP ", 5) == 0)
		obtypename = tag + 5;
	else
		return EVENT_TRIGGER_COMMAND_TAG_NOT_RECOGNIZED;

	/*
	 * ...and the object type should be something recognizable.
	 */
	for (etsd = event_trigger_support; etsd->obtypename != NULL; etsd++)
		if (pg_strcasecmp(etsd->obtypename, obtypename) == 0)
			break;
	if (etsd->obtypename == NULL)
		return EVENT_TRIGGER_COMMAND_TAG_NOT_RECOGNIZED;
	if (!etsd->supported)
		return EVENT_TRIGGER_COMMAND_TAG_NOT_SUPPORTED;
	return EVENT_TRIGGER_COMMAND_TAG_OK;
}

/*
 * Complain about a duplicate filter variable.
 */
static void
error_duplicate_filter_variable(const char *defname)
{
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("filter variable \"%s\" specified more than once",
					defname)));
}

/*
 * Insert the new pg_event_trigger row and record dependencies.
 */
static Oid
insert_event_trigger_tuple(char *trigname, char *eventname, Oid evtOwner,
						   Oid funcoid, List *taglist)
{
	Relation	tgrel;
	Oid			trigoid;
	HeapTuple	tuple;
	Datum		values[Natts_pg_trigger];
	bool		nulls[Natts_pg_trigger];
	NameData	evtnamedata,
				evteventdata;
	ObjectAddress myself,
				referenced;

	/* Open pg_event_trigger. */
	tgrel = heap_open(EventTriggerRelationId, RowExclusiveLock);

	/* Build the new pg_trigger tuple. */
	memset(nulls, false, sizeof(nulls));
	namestrcpy(&evtnamedata, trigname);
	values[Anum_pg_event_trigger_evtname - 1] = NameGetDatum(&evtnamedata);
	namestrcpy(&evteventdata, eventname);
	values[Anum_pg_event_trigger_evtevent - 1] = NameGetDatum(&evteventdata);
	values[Anum_pg_event_trigger_evtowner - 1] = ObjectIdGetDatum(evtOwner);
	values[Anum_pg_event_trigger_evtfoid - 1] = ObjectIdGetDatum(funcoid);
	values[Anum_pg_event_trigger_evtenabled - 1] =
		CharGetDatum(TRIGGER_FIRES_ON_ORIGIN);
	if (taglist == NIL)
		nulls[Anum_pg_event_trigger_evttags - 1] = true;
	else
		values[Anum_pg_event_trigger_evttags - 1] =
			filter_list_to_array(taglist);

	/* Insert heap tuple. */
	tuple = heap_form_tuple(tgrel->rd_att, values, nulls);
	trigoid = simple_heap_insert(tgrel, tuple);
	CatalogUpdateIndexes(tgrel, tuple);
	heap_freetuple(tuple);

	/* Depend on owner. */
	recordDependencyOnOwner(EventTriggerRelationId, trigoid, evtOwner);

	/* Depend on event trigger function. */
	myself.classId = EventTriggerRelationId;
	myself.objectId = trigoid;
	myself.objectSubId = 0;
	referenced.classId = ProcedureRelationId;
	referenced.objectId = funcoid;
	referenced.objectSubId = 0;
	recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* Depend on extension, if any. */
	recordDependencyOnCurrentExtension(&myself, false);

	/* Post creation hook for new event trigger */
	InvokeObjectPostCreateHook(EventTriggerRelationId, trigoid, 0);

	/* Close pg_event_trigger. */
	heap_close(tgrel, RowExclusiveLock);

	return trigoid;
}

/*
 * In the parser, a clause like WHEN tag IN ('cmd1', 'cmd2') is represented
 * by a DefElem whose value is a List of String nodes; in the catalog, we
 * store the list of strings as a text array.  This function transforms the
 * former representation into the latter one.
 *
 * For cleanliness, we store command tags in the catalog as text.  It's
 * possible (although not currently anticipated) that we might have
 * a case-sensitive filter variable in the future, in which case this would
 * need some further adjustment.
 */
static Datum
filter_list_to_array(List *filterlist)
{
	ListCell   *lc;
	Datum	   *data;
	int			i = 0,
				l = list_length(filterlist);

	data = (Datum *) palloc(l * sizeof(Datum));

	foreach(lc, filterlist)
	{
		const char *value = strVal(lfirst(lc));
		char	   *result,
				   *p;

		result = pstrdup(value);
		for (p = result; *p; p++)
			*p = pg_ascii_toupper((unsigned char) *p);
		data[i++] = PointerGetDatum(cstring_to_text(result));
		pfree(result);
	}

	return PointerGetDatum(construct_array(data, l, TEXTOID, -1, false, 'i'));
}

/*
 * Guts of event trigger deletion.
 */
void
RemoveEventTriggerById(Oid trigOid)
{
	Relation	tgrel;
	HeapTuple	tup;

	tgrel = heap_open(EventTriggerRelationId, RowExclusiveLock);

	tup = SearchSysCache1(EVENTTRIGGEROID, ObjectIdGetDatum(trigOid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for event trigger %u", trigOid);

	simple_heap_delete(tgrel, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(tgrel, RowExclusiveLock);
}

/*
 * ALTER EVENT TRIGGER foo ENABLE|DISABLE|ENABLE ALWAYS|REPLICA
 */
Oid
AlterEventTrigger(AlterEventTrigStmt *stmt)
{
	Relation	tgrel;
	HeapTuple	tup;
	Oid			trigoid;
	Form_pg_event_trigger evtForm;
	char		tgenabled = stmt->tgenabled;

	tgrel = heap_open(EventTriggerRelationId, RowExclusiveLock);

	tup = SearchSysCacheCopy1(EVENTTRIGGERNAME,
							  CStringGetDatum(stmt->trigname));
	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("event trigger \"%s\" does not exist",
						stmt->trigname)));

	trigoid = HeapTupleGetOid(tup);

	if (!pg_event_trigger_ownercheck(trigoid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_EVENT_TRIGGER,
					   stmt->trigname);

	/* tuple is a copy, so we can modify it below */
	evtForm = (Form_pg_event_trigger) GETSTRUCT(tup);
	evtForm->evtenabled = tgenabled;

	simple_heap_update(tgrel, &tup->t_self, tup);
	CatalogUpdateIndexes(tgrel, tup);

	InvokeObjectPostAlterHook(EventTriggerRelationId,
							  trigoid, 0);

	/* clean up */
	heap_freetuple(tup);
	heap_close(tgrel, RowExclusiveLock);

	return trigoid;
}

/*
 * Change event trigger's owner -- by name
 */
Oid
AlterEventTriggerOwner(const char *name, Oid newOwnerId)
{
	Oid			evtOid;
	HeapTuple	tup;
	Relation	rel;

	rel = heap_open(EventTriggerRelationId, RowExclusiveLock);

	tup = SearchSysCacheCopy1(EVENTTRIGGERNAME, CStringGetDatum(name));

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("event trigger \"%s\" does not exist", name)));

	evtOid = HeapTupleGetOid(tup);

	AlterEventTriggerOwner_internal(rel, tup, newOwnerId);

	heap_freetuple(tup);

	heap_close(rel, RowExclusiveLock);

	return evtOid;
}

/*
 * Change extension owner, by OID
 */
void
AlterEventTriggerOwner_oid(Oid trigOid, Oid newOwnerId)
{
	HeapTuple	tup;
	Relation	rel;

	rel = heap_open(EventTriggerRelationId, RowExclusiveLock);

	tup = SearchSysCacheCopy1(EVENTTRIGGEROID, ObjectIdGetDatum(trigOid));

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
			   errmsg("event trigger with OID %u does not exist", trigOid)));

	AlterEventTriggerOwner_internal(rel, tup, newOwnerId);

	heap_freetuple(tup);

	heap_close(rel, RowExclusiveLock);
}

/*
 * Internal workhorse for changing an event trigger's owner
 */
static void
AlterEventTriggerOwner_internal(Relation rel, HeapTuple tup, Oid newOwnerId)
{
	Form_pg_event_trigger form;

	form = (Form_pg_event_trigger) GETSTRUCT(tup);

	if (form->evtowner == newOwnerId)
		return;

	if (!pg_event_trigger_ownercheck(HeapTupleGetOid(tup), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_EVENT_TRIGGER,
					   NameStr(form->evtname));

	/* New owner must be a superuser */
	if (!superuser_arg(newOwnerId))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
		  errmsg("permission denied to change owner of event trigger \"%s\"",
				 NameStr(form->evtname)),
			 errhint("The owner of an event trigger must be a superuser.")));

	form->evtowner = newOwnerId;
	simple_heap_update(rel, &tup->t_self, tup);
	CatalogUpdateIndexes(rel, tup);

	/* Update owner dependency reference */
	changeDependencyOnOwner(EventTriggerRelationId,
							HeapTupleGetOid(tup),
							newOwnerId);

	InvokeObjectPostAlterHook(EventTriggerRelationId,
							  HeapTupleGetOid(tup), 0);
}

/*
 * get_event_trigger_oid - Look up an event trigger by name to find its OID.
 *
 * If missing_ok is false, throw an error if trigger not found.  If
 * true, just return InvalidOid.
 */
Oid
get_event_trigger_oid(const char *trigname, bool missing_ok)
{
	Oid			oid;

	oid = GetSysCacheOid1(EVENTTRIGGERNAME, CStringGetDatum(trigname));
	if (!OidIsValid(oid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("event trigger \"%s\" does not exist", trigname)));
	return oid;
}

/*
 * Return true when we want to fire given Event Trigger and false otherwise,
 * filtering on the session replication role and the event trigger registered
 * tags matching.
 */
static bool
filter_event_trigger(const char **tag, EventTriggerCacheItem *item)
{
	/*
	 * Filter by session replication role, knowing that we never see disabled
	 * items down here.
	 */
	if (SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA)
	{
		if (item->enabled == TRIGGER_FIRES_ON_ORIGIN)
			return false;
	}
	else
	{
		if (item->enabled == TRIGGER_FIRES_ON_REPLICA)
			return false;
	}

	/* Filter by tags, if any were specified. */
	if (item->ntags != 0 && bsearch(tag, item->tag,
									item->ntags, sizeof(char *),
									pg_qsort_strcmp) == NULL)
		return false;

	/* if we reach that point, we're not filtering out this item */
	return true;
}

/*
 * Setup for running triggers for the given event.  Return value is an OID list
 * of functions to run; if there are any, trigdata is filled with an
 * appropriate EventTriggerData for them to receive.
 */
static List *
EventTriggerCommonSetup(Node *parsetree,
						EventTriggerEvent event, const char *eventstr,
						EventTriggerData *trigdata)
{
	const char *tag;
	List	   *cachelist;
	ListCell   *lc;
	List	   *runlist = NIL;

	/*
	 * We want the list of command tags for which this procedure is actually
	 * invoked to match up exactly with the list that CREATE EVENT TRIGGER
	 * accepts.  This debugging cross-check will throw an error if this
	 * function is invoked for a command tag that CREATE EVENT TRIGGER won't
	 * accept.  (Unfortunately, there doesn't seem to be any simple, automated
	 * way to verify that CREATE EVENT TRIGGER doesn't accept extra stuff that
	 * never reaches this control point.)
	 *
	 * If this cross-check fails for you, you probably need to either adjust
	 * standard_ProcessUtility() not to invoke event triggers for the command
	 * type in question, or you need to adjust check_ddl_tag to accept the
	 * relevant command tag.
	 */
#ifdef USE_ASSERT_CHECKING
	if (assert_enabled)
	{
		const char *dbgtag;

		dbgtag = CreateCommandTag(parsetree);
		if (check_ddl_tag(dbgtag) != EVENT_TRIGGER_COMMAND_TAG_OK)
			elog(ERROR, "unexpected command tag \"%s\"", dbgtag);
	}
#endif

	/* Use cache to find triggers for this event; fast exit if none. */
	cachelist = EventCacheLookup(event);
	if (cachelist == NIL)
		return NIL;

	/* Get the command tag. */
	tag = CreateCommandTag(parsetree);

	/*
	 * Filter list of event triggers by command tag, and copy them into our
	 * memory context.  Once we start running the command trigers, or indeed
	 * once we do anything at all that touches the catalogs, an invalidation
	 * might leave cachelist pointing at garbage, so we must do this before we
	 * can do much else.
	 */
	foreach(lc, cachelist)
	{
		EventTriggerCacheItem *item = lfirst(lc);

		if (filter_event_trigger(&tag, item))
		{
			/* We must plan to fire this trigger. */
			runlist = lappend_oid(runlist, item->fnoid);
		}
	}

	/* don't spend any more time on this if no functions to run */
	if (runlist == NIL)
		return NIL;

	trigdata->type = T_EventTriggerData;
	trigdata->event = eventstr;
	trigdata->parsetree = parsetree;
	trigdata->tag = tag;

	return runlist;
}

/*
 * Fire ddl_command_start triggers.
 */
void
EventTriggerDDLCommandStart(Node *parsetree)
{
	List	   *runlist;
	EventTriggerData trigdata;

	/*
	 * Event Triggers are completely disabled in standalone mode.  There are
	 * (at least) two reasons for this:
	 *
	 * 1. A sufficiently broken event trigger might not only render the
	 * database unusable, but prevent disabling itself to fix the situation.
	 * In this scenario, restarting in standalone mode provides an escape
	 * hatch.
	 *
	 * 2. BuildEventTriggerCache relies on systable_beginscan_ordered, and
	 * therefore will malfunction if pg_event_trigger's indexes are damaged.
	 * To allow recovery from a damaged index, we need some operating mode
	 * wherein event triggers are disabled.  (Or we could implement
	 * heapscan-and-sort logic for that case, but having disaster recovery
	 * scenarios depend on code that's otherwise untested isn't appetizing.)
	 */
	if (!IsUnderPostmaster)
		return;

	runlist = EventTriggerCommonSetup(parsetree,
									  EVT_DDLCommandStart,
									  "ddl_command_start",
									  &trigdata);
	if (runlist == NIL)
		return;

	/* Run the triggers. */
	EventTriggerInvoke(runlist, &trigdata);

	/* Cleanup. */
	list_free(runlist);

	/*
	 * Make sure anything the event triggers did will be visible to the main
	 * command.
	 */
	CommandCounterIncrement();
}

/*
 * Fire ddl_command_end triggers.
 */
void
EventTriggerDDLCommandEnd(Node *parsetree)
{
	List	   *runlist;
	EventTriggerData trigdata;

	/*
	 * See EventTriggerDDLCommandStart for a discussion about why event
	 * triggers are disabled in single user mode.
	 */
	if (!IsUnderPostmaster)
		return;

	runlist = EventTriggerCommonSetup(parsetree,
									  EVT_DDLCommandEnd, "ddl_command_end",
									  &trigdata);
	if (runlist == NIL)
		return;

	/*
	 * Make sure anything the main command did will be visible to the event
	 * triggers.
	 */
	CommandCounterIncrement();

	/* Run the triggers. */
	EventTriggerInvoke(runlist, &trigdata);

	/* Cleanup. */
	list_free(runlist);
}

/*
 * Fire sql_drop triggers.
 */
void
EventTriggerSQLDrop(Node *parsetree)
{
	List	   *runlist;
	EventTriggerData trigdata;

	/*
	 * See EventTriggerDDLCommandStart for a discussion about why event
	 * triggers are disabled in single user mode.
	 */
	if (!IsUnderPostmaster)
		return;

	/*
	 * Use current state to determine whether this event fires at all.  If
	 * there are no triggers for the sql_drop event, then we don't have
	 * anything to do here.  Note that dropped object collection is disabled
	 * if this is the case, so even if we were to try to run, the list would
	 * be empty.
	 */
	if (!currentEventTriggerState ||
		slist_is_empty(&currentEventTriggerState->SQLDropList))
		return;

	runlist = EventTriggerCommonSetup(parsetree,
									  EVT_SQLDrop, "sql_drop",
									  &trigdata);

	/*
	 * Nothing to do if run list is empty.  Note this shouldn't happen,
	 * because if there are no sql_drop events, then objects-to-drop wouldn't
	 * have been collected in the first place and we would have quitted above.
	 */
	if (runlist == NIL)
		return;

	/*
	 * Make sure anything the main command did will be visible to the event
	 * triggers.
	 */
	CommandCounterIncrement();

	/*
	 * Make sure pg_event_trigger_dropped_objects only works when running
	 * these triggers.  Use PG_TRY to ensure in_sql_drop is reset even when
	 * one trigger fails.  (This is perhaps not necessary, as the currentState
	 * variable will be removed shortly by our caller, but it seems better to
	 * play safe.)
	 */
	currentEventTriggerState->in_sql_drop = true;

	/* Run the triggers. */
	PG_TRY();
	{
		EventTriggerInvoke(runlist, &trigdata);
	}
	PG_CATCH();
	{
		currentEventTriggerState->in_sql_drop = false;
		PG_RE_THROW();
	}
	PG_END_TRY();
	currentEventTriggerState->in_sql_drop = false;

	/* Cleanup. */
	list_free(runlist);
}

/*
 * Invoke each event trigger in a list of event triggers.
 */
static void
EventTriggerInvoke(List *fn_oid_list, EventTriggerData *trigdata)
{
	MemoryContext context;
	MemoryContext oldcontext;
	ListCell   *lc;
	bool		first = true;

	/* Guard against stack overflow due to recursive event trigger */
	check_stack_depth();

	/*
	 * Let's evaluate event triggers in their own memory context, so that any
	 * leaks get cleaned up promptly.
	 */
	context = AllocSetContextCreate(CurrentMemoryContext,
									"event trigger context",
									ALLOCSET_DEFAULT_MINSIZE,
									ALLOCSET_DEFAULT_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(context);

	/* Call each event trigger. */
	foreach(lc, fn_oid_list)
	{
		Oid			fnoid = lfirst_oid(lc);
		FmgrInfo	flinfo;
		FunctionCallInfoData fcinfo;
		PgStat_FunctionCallUsage fcusage;

		/*
		 * We want each event trigger to be able to see the results of the
		 * previous event trigger's action.  Caller is responsible for any
		 * command-counter increment that is needed between the event trigger
		 * and anything else in the transaction.
		 */
		if (first)
			first = false;
		else
			CommandCounterIncrement();

		/* Look up the function */
		fmgr_info(fnoid, &flinfo);

		/* Call the function, passing no arguments but setting a context. */
		InitFunctionCallInfoData(fcinfo, &flinfo, 0,
								 InvalidOid, (Node *) trigdata, NULL);
		pgstat_init_function_usage(&fcinfo, &fcusage);
		FunctionCallInvoke(&fcinfo);
		pgstat_end_function_usage(&fcusage, true);

		/* Reclaim memory. */
		MemoryContextReset(context);
	}

	/* Restore old memory context and delete the temporary one. */
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(context);
}

/*
 * Do event triggers support this object type?
 */
bool
EventTriggerSupportsObjectType(ObjectType obtype)
{
	switch (obtype)
	{
		case OBJECT_DATABASE:
		case OBJECT_TABLESPACE:
		case OBJECT_ROLE:
			/* no support for global objects */
			return false;
		case OBJECT_EVENT_TRIGGER:
			/* no support for event triggers on event triggers */
			return false;
		case OBJECT_AGGREGATE:
		case OBJECT_ATTRIBUTE:
		case OBJECT_CAST:
		case OBJECT_COLUMN:
		case OBJECT_COMPOSITE:
		case OBJECT_CONSTRAINT:
		case OBJECT_COLLATION:
		case OBJECT_CONVERSION:
		case OBJECT_DEFAULT:
		case OBJECT_DOMAIN:
		case OBJECT_EXTENSION:
		case OBJECT_FDW:
		case OBJECT_FOREIGN_SERVER:
		case OBJECT_FOREIGN_TABLE:
		case OBJECT_FUNCTION:
		case OBJECT_INDEX:
		case OBJECT_LANGUAGE:
		case OBJECT_LARGEOBJECT:
		case OBJECT_MATVIEW:
		case OBJECT_OPCLASS:
		case OBJECT_OPERATOR:
		case OBJECT_OPFAMILY:
		case OBJECT_RULE:
		case OBJECT_SCHEMA:
		case OBJECT_SEQUENCE:
		case OBJECT_TABLE:
		case OBJECT_TRIGGER:
		case OBJECT_TSCONFIGURATION:
		case OBJECT_TSDICTIONARY:
		case OBJECT_TSPARSER:
		case OBJECT_TSTEMPLATE:
		case OBJECT_TYPE:
		case OBJECT_USER_MAPPING:
		case OBJECT_VIEW:
			return true;
	}
	return true;
}

/*
 * Do event triggers support this object class?
 */
bool
EventTriggerSupportsObjectClass(ObjectClass objclass)
{
	switch (objclass)
	{
		case OCLASS_DATABASE:
		case OCLASS_TBLSPACE:
		case OCLASS_ROLE:
			/* no support for global objects */
			return false;
		case OCLASS_EVENT_TRIGGER:
			/* no support for event triggers on event triggers */
			return false;
		case OCLASS_CLASS:
		case OCLASS_PROC:
		case OCLASS_TYPE:
		case OCLASS_CAST:
		case OCLASS_COLLATION:
		case OCLASS_CONSTRAINT:
		case OCLASS_CONVERSION:
		case OCLASS_DEFAULT:
		case OCLASS_LANGUAGE:
		case OCLASS_LARGEOBJECT:
		case OCLASS_OPERATOR:
		case OCLASS_OPCLASS:
		case OCLASS_OPFAMILY:
		case OCLASS_AMOP:
		case OCLASS_AMPROC:
		case OCLASS_REWRITE:
		case OCLASS_TRIGGER:
		case OCLASS_SCHEMA:
		case OCLASS_TSPARSER:
		case OCLASS_TSDICT:
		case OCLASS_TSTEMPLATE:
		case OCLASS_TSCONFIG:
		case OCLASS_FDW:
		case OCLASS_FOREIGN_SERVER:
		case OCLASS_USER_MAPPING:
		case OCLASS_DEFACL:
		case OCLASS_EXTENSION:
			return true;

		case MAX_OCLASS:

			/*
			 * This shouldn't ever happen, but we keep the case to avoid a
			 * compiler warning without a "default" clause in the switch.
			 */
			Assert(false);
			break;
	}

	return true;
}

bool
EventTriggerSupportsGrantObjectType(GrantObjectType objtype)
{
	switch (objtype)
	{
		case ACL_OBJECT_DATABASE:
		case ACL_OBJECT_TABLESPACE:
			/* no support for global objects */
			return false;

		case ACL_OBJECT_COLUMN:
		case ACL_OBJECT_RELATION:
		case ACL_OBJECT_SEQUENCE:
		case ACL_OBJECT_DOMAIN:
		case ACL_OBJECT_FDW:
		case ACL_OBJECT_FOREIGN_SERVER:
		case ACL_OBJECT_FUNCTION:
		case ACL_OBJECT_LANGUAGE:
		case ACL_OBJECT_LARGEOBJECT:
		case ACL_OBJECT_NAMESPACE:
		case ACL_OBJECT_TYPE:
			return true;
		default:
			Assert(false);
			return true;
	}
}

/*
 * Prepare event trigger state for a new complete query to run, if necessary;
 * returns whether this was done.  If it was, EventTriggerEndCompleteQuery must
 * be called when the query is done, regardless of whether it succeeds or fails
 * -- so use of a PG_TRY block is mandatory.
 */
bool
EventTriggerBeginCompleteQuery(void)
{
	EventTriggerQueryState *state;
	MemoryContext cxt;

	cxt = AllocSetContextCreate(TopMemoryContext,
								"event trigger state",
								ALLOCSET_DEFAULT_MINSIZE,
								ALLOCSET_DEFAULT_INITSIZE,
								ALLOCSET_DEFAULT_MAXSIZE);
	state = MemoryContextAlloc(cxt, sizeof(EventTriggerQueryState));
	state->cxt = cxt;
	slist_init(&(state->SQLDropList));
	state->in_sql_drop = false;
	state->curcmd = NULL;
	state->stash = NIL;
	state->in_extension = currentEventTriggerState ? currentEventTriggerState->in_extension : false;

	state->previous = currentEventTriggerState;
	currentEventTriggerState = state;

	return true;
}

/*
 * Query completed (or errored out) -- clean up local state, return to previous
 * one.
 *
 * Note: it's an error to call this routine if EventTriggerBeginCompleteQuery
 * returned false previously.
 *
 * Note: this might be called in the PG_CATCH block of a failing transaction,
 * so be wary of running anything unnecessary.  (In particular, it's probably
 * unwise to try to allocate memory.)
 */
void
EventTriggerEndCompleteQuery(void)
{
	EventTriggerQueryState *prevstate;

	prevstate = currentEventTriggerState->previous;

	/* this avoids the need for retail pfree of SQLDropList items: */
	MemoryContextDelete(currentEventTriggerState->cxt);

	currentEventTriggerState = prevstate;
}

/*
 * Do we need to keep close track of objects being dropped?
 *
 * This is useful because there is a cost to running with them enabled.
 */
bool
trackDroppedObjectsNeeded(void)
{
	/* true if any sql_drop event trigger exists */
	return list_length(EventCacheLookup(EVT_SQLDrop)) > 0;
}

/*
 * Support for dropped objects information on event trigger functions.
 *
 * We keep the list of objects dropped by the current command in current
 * state's SQLDropList (comprising SQLDropObject items).  Each time a new
 * command is to start, a clean EventTriggerQueryState is created; commands
 * that drop objects do the dependency.c dance to drop objects, which
 * populates the current state's SQLDropList; when the event triggers are
 * invoked they can consume the list via pg_event_trigger_dropped_objects().
 * When the command finishes, the EventTriggerQueryState is cleared, and
 * the one from the previous command is restored (when no command is in
 * execution, the current state is NULL).
 *
 * All this lets us support the case that an event trigger function drops
 * objects "reentrantly".
 */

/*
 * Register one object as being dropped by the current command.
 */
void
EventTriggerSQLDropAddObject(const ObjectAddress *object, bool original, bool normal)
{
	SQLDropObject *obj;
	MemoryContext oldcxt;

	if (!currentEventTriggerState)
		return;

	Assert(EventTriggerSupportsObjectClass(getObjectClass(object)));

	/* don't report temp schemas */
	if (object->classId == NamespaceRelationId &&
		isAnyTempNamespace(object->objectId))
		return;

	oldcxt = MemoryContextSwitchTo(currentEventTriggerState->cxt);

	obj = palloc0(sizeof(SQLDropObject));
	obj->address = *object;
	obj->original = original;
	obj->normal = normal;

	/*
	 * Obtain schema names from the object's catalog tuple, if one exists;
	 * this lets us skip objects in temp schemas.  We trust that
	 * ObjectProperty contains all object classes that can be
	 * schema-qualified.
	 */
	if (is_objectclass_supported(object->classId))
	{
		Relation	catalog;
		HeapTuple	tuple;

		catalog = heap_open(obj->address.classId, AccessShareLock);
		tuple = get_catalog_object_by_oid(catalog, obj->address.objectId);

		if (tuple)
		{
			AttrNumber	attnum;
			Datum		datum;
			bool		isnull;

			attnum = get_object_attnum_namespace(obj->address.classId);
			if (attnum != InvalidAttrNumber)
			{
				datum = heap_getattr(tuple, attnum,
									 RelationGetDescr(catalog), &isnull);
				if (!isnull)
				{
					Oid			namespaceId;

					namespaceId = DatumGetObjectId(datum);
					/* Don't report objects in temp namespaces */
					if (isAnyTempNamespace(namespaceId))
					{
						pfree(obj);
						heap_close(catalog, AccessShareLock);
						MemoryContextSwitchTo(oldcxt);
						return;
					}

					obj->schemaname = get_namespace_name(namespaceId);
				}
			}

			if (get_object_namensp_unique(obj->address.classId) &&
				obj->address.objectSubId == 0)
			{
				attnum = get_object_attnum_name(obj->address.classId);
				if (attnum != InvalidAttrNumber)
				{
					datum = heap_getattr(tuple, attnum,
										 RelationGetDescr(catalog), &isnull);
					if (!isnull)
						obj->objname = pstrdup(NameStr(*DatumGetName(datum)));
				}
			}
		}

		heap_close(catalog, AccessShareLock);
	}

	/* object identity, objname and objargs */
	obj->objidentity =
		getObjectIdentityParts(&obj->address, &obj->addrnames, &obj->addrargs);

	/* object type */
	obj->objecttype = getObjectTypeDescription(&obj->address);

	slist_push_head(&(currentEventTriggerState->SQLDropList), &obj->next);

	MemoryContextSwitchTo(oldcxt);
}

/*
 * pg_event_trigger_dropped_objects
 *
 * Make the list of dropped objects available to the user function run by the
 * Event Trigger.
 */
Datum
pg_event_trigger_dropped_objects(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	slist_iter	iter;

	/*
	 * Protect this function from being called out of context
	 */
	if (!currentEventTriggerState ||
		!currentEventTriggerState->in_sql_drop)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		 errmsg("%s can only be called in a sql_drop event trigger function",
				"pg_event_trigger_dropped_objects()")));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Build tuplestore to hold the result rows */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	slist_foreach(iter, &(currentEventTriggerState->SQLDropList))
	{
		SQLDropObject *obj;
		int			i = 0;
		Datum		values[11];
		bool		nulls[11];

		obj = slist_container(SQLDropObject, next, iter.cur);

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		/* classid */
		values[i++] = ObjectIdGetDatum(obj->address.classId);

		/* objid */
		values[i++] = ObjectIdGetDatum(obj->address.objectId);

		/* objsubid */
		values[i++] = Int32GetDatum(obj->address.objectSubId);

		/* original */
		values[i++] = BoolGetDatum(obj->original);

		/* normal */
		values[i++] = BoolGetDatum(obj->normal);

		/* object_type */
		values[i++] = CStringGetTextDatum(obj->objecttype);

		/* schema_name */
		if (obj->schemaname)
			values[i++] = CStringGetTextDatum(obj->schemaname);
		else
			nulls[i++] = true;

		/* object_name */
		if (obj->objname)
			values[i++] = CStringGetTextDatum(obj->objname);
		else
			nulls[i++] = true;

		/* object_identity */
		if (obj->objidentity)
			values[i++] = CStringGetTextDatum(obj->objidentity);
		else
			nulls[i++] = true;

		/* address_names */
		if (obj->addrnames)
		{
			ArrayType *arr;
			Datum	*datums;
			int		j = 0;
			ListCell *cell;

			datums = palloc(sizeof(text *) * list_length(obj->addrnames));
			foreach(cell, obj->addrnames)
			{
				char   *name = lfirst(cell);

				datums[j++] = CStringGetTextDatum(name);
			}

			arr = construct_array(datums, list_length(obj->addrnames),
								  TEXTOID, -1, false, 'i');

			values[i++] = PointerGetDatum(arr);
		}
		else
			nulls[i++] = true;

		/* address_args */
		/* FIXME duplicated code block ... */
		if (obj->addrargs)
		{
			ArrayType *arr;
			Datum   *datums;
			int		j = 0;
			ListCell *cell;

			datums = palloc(sizeof(text *) * list_length(obj->addrargs));
			foreach(cell, obj->addrargs)
			{
				char	*arg = lfirst(cell);

				datums[j++] = CStringGetTextDatum(arg);
			}

			arr = construct_array(datums, list_length(obj->addrargs),
								  TEXTOID, -1, false, 'i');
			values[i++] = PointerGetDatum(arr);
		}
		else
			nulls[i++] = true;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * EventTriggerStashCommand
 * 		Save data about a simple DDL command that was just executed
 */
void
EventTriggerStashCommand(Oid objectId, uint32 objectSubId, ObjectType objtype,
						 Node *parsetree)
{
	MemoryContext oldcxt;
	StashedCommand *stashed;

	oldcxt = MemoryContextSwitchTo(currentEventTriggerState->cxt);

	stashed = palloc(sizeof(StashedCommand));

	stashed->type = SCT_Basic;
	stashed->in_extension = currentEventTriggerState->in_extension;

	stashed->d.basic.objectId = objectId;
	stashed->d.basic.objtype = objtype;
	stashed->d.basic.objectSubId = objectSubId;
	stashed->parsetree = copyObject(parsetree);

	currentEventTriggerState->stash = lappend(currentEventTriggerState->stash,
											  stashed);

	MemoryContextSwitchTo(oldcxt);
}

void
EventTriggerStashExtensionStart(void)
{
	currentEventTriggerState->in_extension = true;
}

void
EventTriggerStashExtensionStop(void)
{
	if (currentEventTriggerState)
		currentEventTriggerState->in_extension = false;
}

/*
 * EventTriggerStartRecordingSubcmds
 * 		Prepare to receive data on a complex DDL command about to be executed
 *
 * Note we don't actually stash the object we create here into the "stashed"
 * list; instead we keep it in curcmd, and only when we're done processing the
 * subcommands we will add it to the actual stash.
 *
 * FIXME -- this API isn't considering the possibility of an ALTER TABLE command
 * being called reentrantly by an event trigger function.  Do we need stackable
 * commands at this level?
 */
void
EventTriggerComplexCmdStart(Node *parsetree, ObjectType objtype)
{
	MemoryContext	oldcxt;
	StashedCommand *stashed;

	oldcxt = MemoryContextSwitchTo(currentEventTriggerState->cxt);

	stashed = palloc(sizeof(StashedCommand));

	stashed->type = SCT_AlterTable;
	stashed->in_extension = currentEventTriggerState->in_extension;

	stashed->d.alterTable.objectId = InvalidOid;
	stashed->d.alterTable.objtype = objtype;
	stashed->d.alterTable.subcmds = NIL;
	/* XXX is it necessary to have the whole parsetree? probably not ... */
	stashed->parsetree = copyObject(parsetree);

	currentEventTriggerState->curcmd = stashed;

	MemoryContextSwitchTo(oldcxt);
}

void
EventTriggerComplexCmdSetOid(Oid objectId)
{
	currentEventTriggerState->curcmd->d.alterTable.objectId = objectId;
}

/*
 * EventTriggerRecordSubcmd
 * 		Save data about a single part of a complex DDL command
 *
 * Right now we only support ALTER TABLE; there are no other DDL commands that
 * require this.  (ALTER TYPE can also generate multiple subcommands, but it's
 * actually parsed as ALTER TABLE, so there is no difference at this level.)
 */
void
EventTriggerRecordSubcmd(Node *subcmd, Oid relid, AttrNumber attnum,
						 Oid newoid)
{
	MemoryContext	oldcxt;
	StashedATSubcmd *newsub;

	Assert(IsA(subcmd, AlterTableCmd));
	Assert(OidIsValid(currentEventTriggerState->curcmd->d.alterTable.objectId));

	/*
	 * If we receive a subcommand intended for a relation other than the one
	 * we've started the complex command for, ignore it.  This is chiefly
	 * concerned with inheritance situations: in such cases, alter table
	 * would dispatch multiple copies of the same command for various things,
	 * but we're only concerned with the one for the main table.
	 */
	if (relid != currentEventTriggerState->curcmd->d.alterTable.objectId)
		return;

	oldcxt = MemoryContextSwitchTo(currentEventTriggerState->cxt);

	newsub = palloc(sizeof(StashedATSubcmd));
	newsub->attnum = attnum;
	newsub->oid = newoid;
	newsub->parsetree = copyObject(subcmd);

	currentEventTriggerState->curcmd->d.alterTable.subcmds =
		lappend(currentEventTriggerState->curcmd->d.alterTable.subcmds, newsub);

	MemoryContextSwitchTo(oldcxt);
}

/*
 * EventTriggerEndRecordingSubcmds
 * 		Finish up saving a complex DDL command
 *
 * FIXME this API isn't considering the possibility that a xact/subxact is
 * aborted partway through.  Probably it's best to add an
 * AtEOSubXact_EventTriggers() to fix this.
 */
void
EventTriggerComplexCmdEnd(void)
{
	/* If no subcommands, don't stash anything */
	if (list_length(currentEventTriggerState->curcmd->d.alterTable.subcmds) != 0)
	{
		currentEventTriggerState->stash =
			lappend(currentEventTriggerState->stash,
					currentEventTriggerState->curcmd);
	}
	else
		pfree(currentEventTriggerState->curcmd);

	currentEventTriggerState->curcmd = NULL;
}

/*
 * EventTriggerStashGrant
 * 		Save data about a GRANT/REVOKE command being executed
 *
 * This function creates a copy of the InternalGrant, as the original might
 * not have the right lifetime.
 */
void
EventTriggerStashGrant(InternalGrant *istmt)
{
	MemoryContext oldcxt;
	StashedCommand *stashed;
	InternalGrant  *icopy;
	ListCell	   *cell;

	oldcxt = MemoryContextSwitchTo(currentEventTriggerState->cxt);

	/*
	 * copying the node is moderately challenging ... XXX should we consider
	 * changing InternalGrant into a full-fledged node instead?
	 */
	icopy = palloc(sizeof(InternalGrant));
	memcpy(icopy, istmt, sizeof(InternalGrant));
	icopy->objects = list_copy(istmt->objects);
	icopy->grantees = list_copy(istmt->grantees);
	icopy->col_privs = NIL;
	foreach(cell, istmt->col_privs)
		icopy->col_privs = lappend(icopy->col_privs, copyObject(lfirst(cell)));

	stashed = palloc(sizeof(StashedCommand));
	stashed->type = SCT_Grant;
	stashed->in_extension = currentEventTriggerState->in_extension;

	stashed->d.grant.istmt = icopy;
	stashed->parsetree = NULL;

	currentEventTriggerState->stash = lappend(currentEventTriggerState->stash,
											  stashed);

	MemoryContextSwitchTo(oldcxt);
}

Datum
pg_event_trigger_get_creation_commands(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	ListCell   *lc;

	/*
	 * Protect this function from being called out of context
	 */
	if (!currentEventTriggerState)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("%s can only be called in an event trigger function",
						PG_FUNCNAME_MACRO)));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Build tuplestore to hold the result rows */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	foreach(lc, currentEventTriggerState->stash)
	{
		StashedCommand *cmd = lfirst(lc);
		char	   *command;

		/*
		 * For IF NOT EXISTS commands that attempt to create an existing
		 * object, the returned OID is Invalid; in those cases, return an empty
		 * command instead of trying to soldier on.
		 *
		 * XXX an alternative would be to look up the Oid of the existing
		 * object and run the deparse with that.  But since the parse tree
		 * might be different from the one that created the object in the first
		 * place, we might not end up in a consistent state anyway.
		 */
		if (cmd->type == SCT_Basic &&
			!OidIsValid(cmd->d.basic.objectId))
			continue;

		command = deparse_utility_command(cmd);

		/*
		 * Some parse trees return NULL when deparse is attempted; we don't
		 * emit anything for them.
		 */
		if (command != NULL)
		{
			Datum		values[9];
			bool		nulls[9];
			ObjectAddress addr;
			int			i = 0;

			MemSet(nulls, 0, sizeof(nulls));

			if (cmd->type == SCT_Basic ||
				cmd->type == SCT_AlterTable)
			{
				Oid			classId;
				Oid			objId;
				uint32		objSubId;
				const char *tag;
				char	   *identity;
				char	   *type;
				char	   *schema = NULL;

				if (cmd->type == SCT_Basic)
				{
					classId = get_objtype_catalog_oid(cmd->d.basic.objtype);
					objId = cmd->d.basic.objectId;
					objSubId = cmd->d.basic.objectSubId;
				}
				else if (cmd->type == SCT_AlterTable)
				{
					classId = get_objtype_catalog_oid(cmd->d.alterTable.objtype);
					objId = cmd->d.alterTable.objectId;
					objSubId = 0;
				}

				tag = CreateCommandTag(cmd->parsetree);
				addr.classId = classId;
				addr.objectId = objId;
				addr.objectSubId = objSubId;

				type = getObjectTypeDescription(&addr);
				identity = getObjectIdentity(&addr);

				/*
				 * Obtain schema name, if any ("pg_temp" if a temp object)
				 */
				if (is_objectclass_supported(addr.classId))
				{
					AttrNumber	nspAttnum;

					nspAttnum = get_object_attnum_namespace(addr.classId);
					if (nspAttnum != InvalidAttrNumber)
					{
						Relation	catalog;
						HeapTuple	objtup;
						Oid			schema_oid;
						bool		isnull;

						catalog = heap_open(addr.classId, AccessShareLock);
						objtup = get_catalog_object_by_oid(catalog,
														   addr.objectId);
						if (!HeapTupleIsValid(objtup))
							elog(ERROR, "cache lookup failed for object %u/%u",
								 addr.classId, addr.objectId);
						schema_oid = heap_getattr(objtup, nspAttnum,
												  RelationGetDescr(catalog), &isnull);
						if (isnull)
							elog(ERROR, "invalid null namespace in object %u/%u/%d",
								 addr.classId, addr.objectId, addr.objectSubId);
						if (isAnyTempNamespace(schema_oid))
							schema = pstrdup("pg_temp");
						else
							schema = get_namespace_name(schema_oid);

						heap_close(catalog, AccessShareLock);
					}
				}

				/* classid */
				values[i++] = ObjectIdGetDatum(addr.classId);
				/* objid */
				values[i++] = ObjectIdGetDatum(addr.objectId);
				/* objsubid */
				values[i++] = Int32GetDatum(addr.objectSubId);
				/* command tag */
				values[i++] = CStringGetTextDatum(tag);
				/* object_type */
				values[i++] = CStringGetTextDatum(type);
				/* schema */
				if (schema == NULL)
					nulls[i++] = true;
				else
					values[i++] = CStringGetTextDatum(schema);
				/* identity */
				values[i++] = CStringGetTextDatum(identity);
				/* in_extension */
				values[i++] = BoolGetDatum(cmd->in_extension);
				/* command */
				values[i++] = CStringGetTextDatum(command);
			}
			else
			{
				Assert(cmd->type == SCT_Grant);

				/* classid */
				nulls[i++] = true;
				/* objid */
				nulls[i++] = true;
				/* objsubid */
				nulls[i++] = true;
				/* command tag */
				values[i++] = CStringGetTextDatum("GRANT");	/* XXX maybe REVOKE or something else */
				/* object_type */
				values[i++] = CStringGetTextDatum("TABLE"); /* XXX maybe something else */
				/* schema */
				nulls[i++] = true;
				/* identity */
				nulls[i++] = true;
				/* in_extension */
				values[i++] = BoolGetDatum(cmd->in_extension);
				/* command */
				values[i++] = CStringGetTextDatum(command);
			}

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	PG_RETURN_VOID();
}

/* ************************* JSON STUFF FROM HERE ************************* *
 *	Code below is used to decode blobs returned by deparse_utility_command	*
 *																			*/

/*
 * Note we only support types that are valid in command representation from
 * deparse_utility_command.
 */
typedef enum
{
	JsonTypeArray,
	JsonTypeObject,
	JsonTypeString,
	JsonTypeNumber
} JsonType;

typedef enum
{
	SpecTypename,
	SpecOperatorname,
	SpecDottedName,
	SpecString,
	SpecNumber,
	SpecStringLiteral,
	SpecIdentifier
} convSpecifier;

/*
 * Extract the named json field, which must be of type string, from the given
 * JSON datum, which must be of type object.  If the field doesn't exist,
 * NULL is returned.  Otherwise the string value is returned.
 */
static char *
expand_get_strval(Datum json, char *field_name)
{
	FunctionCallInfoData fcinfo;
	Datum		result;
	char	   *value_str;

	InitFunctionCallInfoData(fcinfo, NULL, 2, InvalidOid, NULL, NULL);

	fcinfo.arg[0] = json;
	fcinfo.argnull[0] = false;
	fcinfo.arg[1] = CStringGetTextDatum(field_name);
	fcinfo.argnull[1] = false;

	result = (*json_object_field_text) (&fcinfo);

	if (fcinfo.isnull)
		return NULL;

	value_str = TextDatumGetCString(result);

	pfree(DatumGetPointer(result));

	return value_str;
}

/*
 * Extract the named json field, which must be of type boolean, from the given
 * JSON datum, which must be of type object.  If the field doesn't exist,
 * isnull is set to TRUE and the return value should not be consulted.
 * Otherwise the boolean value is returned.
 */
static bool
expand_get_boolval(Datum json, char *field_name, bool *isnull)
{
	FunctionCallInfoData fcinfo;
	Datum		result;
	char	   *value_str;

	*isnull = false;

	InitFunctionCallInfoData(fcinfo, NULL, 2, InvalidOid, NULL, NULL);

	fcinfo.arg[0] = json;
	fcinfo.argnull[0] = false;
	fcinfo.arg[1] = CStringGetTextDatum(field_name);
	fcinfo.argnull[1] = false;

	result = (*json_object_field_text) (&fcinfo);

	if (fcinfo.isnull)
	{
		*isnull = true;
		return false;
	}

	value_str = TextDatumGetCString(result);

	if (strcmp(value_str, "true") == 0)
		return true;

	Assert(strcmp(value_str, "false") == 0);
	return false;
}

/*
 * Given a JSON value, return its type.
 *
 * We return both a JsonType (for easy control flow), and a string name (for
 * error reporting).
 */
static JsonType
jsonval_get_type(Datum jsonval, char **typename)
{
	JsonType	json_elt_type;
	Datum		paramtype_datum;
	char	   *paramtype;

	paramtype_datum = DirectFunctionCall1(json_typeof, jsonval);
	paramtype = TextDatumGetCString(paramtype_datum);

	if (strcmp(paramtype, "array") == 0)
		json_elt_type = JsonTypeArray;
	else if (strcmp(paramtype, "object") == 0)
		json_elt_type = JsonTypeObject;
	else if (strcmp(paramtype, "string") == 0)
		json_elt_type = JsonTypeString;
	else if (strcmp(paramtype, "number") == 0)
		json_elt_type = JsonTypeNumber;
	else
		/* XXX improve this; need to specify array index or param name */
		elog(ERROR, "unexpected JSON element type %s",
			 paramtype);

	if (typename)
		*typename = pstrdup(paramtype);

	return json_elt_type;
}

/*
 * dequote_jsonval
 *		Take a string value extracted from a JSON object, and return a copy of it
 *		with the quoting removed.
 *
 * Another alternative to this would be to run the extraction routine again,
 * using the "_text" variant which returns the value without quotes; but this
 * is expensive, and moreover it complicates the logic a lot because not all
 * values are extracted in the same way (some are extracted using
 * json_object_field, others using json_array_element).  Dequoting the object
 * already at hand is a lot easier.
 */
static char *
dequote_jsonval(char *jsonval)
{
	char	   *result;
	int			inputlen = strlen(jsonval);
	int			i;
	int			j = 0;

	result = palloc(strlen(jsonval) + 1);

	/* skip the start and end quotes right away */
	for (i = 1; i < inputlen - 1; i++)
	{
		if (jsonval[i] == '\\')
		{
			i++;

			/* This uses same logic as json.c */
			switch (jsonval[i])
			{
				case 'b':
					result[j++] = '\b';
					continue;
				case 'f':
					result[j++] = '\f';
					continue;
				case 'n':
					result[j++] = '\n';
					continue;
				case 'r':
					result[j++] = '\r';
					continue;
				case 't':
					result[j++] = '\t';
					continue;
				case '"':
				case '\\':
				case '/':
					break;
				default:
					/* XXX: ERROR? */
					break;
			}
		}

		result[j++] = jsonval[i];
	}
	result[j] = '\0';

	return result;
}

/*
 * Expand a json value as an identifier.  The value must be of type string.
 */
static void
expand_jsonval_identifier(StringInfo buf, Datum jsonval)
{
	char	   *unquoted;

	unquoted = dequote_jsonval(TextDatumGetCString(jsonval));
	appendStringInfo(buf, "%s", quote_identifier(unquoted));

	pfree(unquoted);
}

/*
 * Expand a json value as a dotted-name.  The value must be of type object
 * and must contain elements "schemaname" (optional), "objname" (mandatory),
 * "attrname" (optional).
 *
 * XXX do we need a "catalogname" as well?
 */
static void
expand_jsonval_dottedname(StringInfo buf, Datum jsonval)
{
	char	   *schema;
	char	   *objname;
	char	   *attrname;
	const char *qschema;
	const char *qname;

	schema = expand_get_strval(jsonval, "schemaname");
	objname = expand_get_strval(jsonval, "objname");
	if (objname == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid NULL object name in %%D element")));
	qname = quote_identifier(objname);
	if (schema == NULL)
	{
		appendStringInfo(buf, "%s", qname);
	}
	else
	{
		qschema = quote_identifier(schema);
		appendStringInfo(buf, "%s.%s",
						 qschema, qname);
		if (qschema != schema)
			pfree((char *) qschema);
		pfree(schema);
	}

	attrname = expand_get_strval(jsonval, "attrname");
	if (attrname)
	{
		const char *qattr;

		qattr = quote_identifier(attrname);
		appendStringInfo(buf, ".%s", qattr);
		if (qattr != attrname)
			pfree((char *) qattr);
		pfree(attrname);
	}

	if (qname != objname)
		pfree((char *) qname);
	pfree(objname);
}

/*
 * expand a json value as a type name.
 */
static void
expand_jsonval_typename(StringInfo buf, Datum jsonval)
{
	char	   *schema = NULL;
	char	   *typename;
	char	   *typmodstr;
	bool		array_isnull;
	bool		is_array;

	typename = expand_get_strval(jsonval, "typename");
	if (typename == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid NULL type name in %%T element")));
	typmodstr = expand_get_strval(jsonval, "typmod");	/* OK if null */
	is_array = expand_get_boolval(jsonval, "is_array", &array_isnull);
	schema = expand_get_strval(jsonval, "schemaname");

	/*
	 * If schema is NULL, then don't schema qualify, but do quote the type
	 * name; if the schema is empty instead, then we don't quote the type name.
	 * This is our (admittedly quite ugly) way of dealing with type names that
	 * might require special treatment.
	 */
	if (schema == NULL)
		appendStringInfo(buf, "%s%s%s",
						 quote_identifier(typename),
						 typmodstr ? typmodstr : "",
						 is_array ? "[]" : "");
	else if (schema[0] == '\0')
		appendStringInfo(buf, "%s%s%s",
						 typename,
						 typmodstr ? typmodstr : "",
						 is_array ? "[]" : "");
	else
		appendStringInfo(buf, "%s.%s%s%s",
						 quote_identifier(schema),
						 quote_identifier(typename),
						 typmodstr ? typmodstr : "",
						 is_array ? "[]" : "");
}

/*
 * Expand a json value as an operator name
 */
static void
expand_jsonval_operator(StringInfo buf, Datum jsonval)
{
	char	   *schema = NULL;
	char	   *operator;

	operator = expand_get_strval(jsonval, "objname");
	if (operator == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid NULL operator name in %%O element")));
	schema = expand_get_strval(jsonval, "schemaname");

	/* schema might be NULL or empty */
	if (schema == NULL || schema[0] == '\0')
		appendStringInfo(buf, "%s", operator);
	else
		appendStringInfo(buf, "%s.%s",
						 quote_identifier(schema),
						 operator);
}

/*
 * Expand a json value as a string.  The value must be of type string or of
 * type object, in which case it must contain a "fmt" element which will be
 * recursively expanded; also, if the object contains an element "present"
 * and it is set to false, the expansion is the empty string.
 */
static void
expand_jsonval_string(StringInfo buf, Datum jsonval, JsonType json_elt_type)
{
	if (json_elt_type == JsonTypeString)
	{
		char	   *str;
		char	   *unquoted;

		str = TextDatumGetCString(jsonval);
		unquoted = dequote_jsonval(str);
		appendStringInfo(buf, "%s", unquoted);
		pfree(str);
		pfree(unquoted);
	}
	else if (json_elt_type == JsonTypeObject)
	{
		bool		present;
		bool		isnull;

		present = expand_get_boolval(jsonval, "present", &isnull);

		if (isnull || present)
		{
			Datum		inner;
			char	   *str;

			inner = DirectFunctionCall1(pg_event_trigger_expand_command,
										jsonval);
			str = TextDatumGetCString(inner);

			appendStringInfoString(buf, str);
			pfree(DatumGetPointer(inner));
			pfree(str);
		}
	}
}

static void
expand_jsonval_number(StringInfo buf, Datum jsonval)
{
	appendStringInfoString(buf, TextDatumGetCString(jsonval));
}

/*
 * Expand a json value as a string literal
 */
static void
expand_jsonval_strlit(StringInfo buf, Datum jsonval)
{
	char   *str;
	char   *unquoted;
	StringInfoData dqdelim;
	static const char dqsuffixes[] = "_XYZZYX_";
	int         dqnextchar = 0;

	/* obtain the string, and remove the JSON quotes and stuff */
	str = TextDatumGetCString(jsonval);
	unquoted = dequote_jsonval(str);

	/* easy case: if there are no ' and no \, just use a single quote */
	if (strchr(unquoted, '\'') == NULL &&
		strchr(unquoted, '\\') == NULL)
	{
		appendStringInfo(buf, "'%s'", unquoted);
		return;
	}

	/* Find a useful dollar-quote delimiter */
	initStringInfo(&dqdelim);
	appendStringInfoString(&dqdelim, "$");
	while (strstr(unquoted, dqdelim.data) != NULL)
	{
		appendStringInfoChar(&dqdelim, dqsuffixes[dqnextchar++]);
		dqnextchar %= sizeof(dqsuffixes) - 1;
	}
	/* add trailing $ */
	appendStringInfoChar(&dqdelim, '$');

	/* And finally produce the quoted literal into the output StringInfo */
	appendStringInfo(buf, "%s%s%s", dqdelim.data, unquoted, dqdelim.data);
}

/*
 * Expand one json element according to rules.
 */
static void
expand_one_element(StringInfo buf, char *param,
				   Datum jsonval, char *valtype, JsonType json_elt_type,
				   convSpecifier specifier)
{
	/*
	 * Validate the parameter type.  If dotted-name was specified, then a JSON
	 * object element is expected; if an identifier was specified, then a JSON
	 * string is expected.	If a string was specified, then either a JSON
	 * object or a string is expected.
	 */
	if (specifier == SpecDottedName && json_elt_type != JsonTypeObject)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("expected JSON object for %%D element \"%s\", got %s",
						param, valtype)));
	if (specifier == SpecTypename && json_elt_type != JsonTypeObject)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("expected JSON object for %%T element \"%s\", got %s",
						param, valtype)));
	if (specifier == SpecOperatorname && json_elt_type != JsonTypeObject)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("expected JSON object for %%O element \"%s\", got %s",
						param, valtype)));
	if (specifier == SpecIdentifier && json_elt_type != JsonTypeString)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("expected JSON string for %%I element \"%s\", got %s",
						param, valtype)));
	if (specifier == SpecStringLiteral && json_elt_type != JsonTypeString)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("expected JSON string for %%L element \"%s\", got %s",
						param, valtype)));
	if (specifier == SpecString &&
		json_elt_type != JsonTypeString && json_elt_type != JsonTypeObject)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("expected JSON string or object for %%s element \"%s\", got %s",
						param, valtype)));
	if (specifier == SpecNumber && json_elt_type != JsonTypeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("expected JSON string or object for %%d element \"%s\", got %s",
						param, valtype)));

	switch (specifier)
	{
		case SpecIdentifier:
			expand_jsonval_identifier(buf, jsonval);
			break;

		case SpecDottedName:
			expand_jsonval_dottedname(buf, jsonval);
			break;

		case SpecString:
			expand_jsonval_string(buf, jsonval, json_elt_type);
			break;

		case SpecStringLiteral:
			expand_jsonval_strlit(buf, jsonval);
			break;

		case SpecNumber:
			expand_jsonval_number(buf, jsonval);
			break;

		case SpecTypename:
			expand_jsonval_typename(buf, jsonval);
			break;

		case SpecOperatorname:
			expand_jsonval_operator(buf, jsonval);
			break;
	}
}

/*
 * Expand one JSON array element according to rules.
 */
static void
expand_one_array_element(StringInfo buf, Datum array, int idx, char *param,
						 convSpecifier specifier)
{
	Datum		elemval;
	JsonType	json_elt_type;
	char	   *elemtype;

	elemval = DirectFunctionCall2(json_array_element,
								  PointerGetDatum(array),
								  Int32GetDatum(idx));
	json_elt_type = jsonval_get_type(elemval, &elemtype);

	expand_one_element(buf, param,
					   elemval, elemtype, json_elt_type,
					   specifier);
}

#define ADVANCE_PARSE_POINTER(ptr,end_ptr) \
	do { \
		if (++(ptr) >= (end_ptr)) \
			ereport(ERROR, \
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
					 errmsg("unterminated format specifier"))); \
	} while (0)

/*------
 * Returns a formatted string from a JSON object.
 *
 * The starting point is the element named "fmt" (which must be a string).
 * This format string may contain zero or more %-escapes, which consist of an
 * element name enclosed in { }, possibly followed by a conversion modifier,
 * followed by a conversion specifier.	Possible conversion specifiers are:
 *
 * %		expand to a literal %.
 * I		expand as a single, non-qualified identifier
 * D		expand as a possibly-qualified identifier
 * T		expand as a type name
 * O		expand as an operator name
 * L		expand as a string literal (quote using single quotes)
 * s		expand as a simple string (no quoting)
 * d		expand as a simple number (no quoting)
 *
 * The element name may have an optional separator specification preceded
 * by a colon.	Its presence indicates that the element is expected to be
 * an array; the specified separator is used to join the array elements.
 *
 * XXX the current implementation works fine, but is likely to be slow: for
 * each element found in the fmt string we parse the JSON object once.	It
 * might be better to use jsonapi.h directly so that we build a hash or tree of
 * elements and their values once before parsing the fmt string, and later scan
 * fmt using the tree.
 *------
 */
Datum
pg_event_trigger_expand_command(PG_FUNCTION_ARGS)
{
	text	   *json = PG_GETARG_TEXT_P(0);
	char	   *fmt_str;
	int			fmt_len;
	const char *cp;
	const char *start_ptr;
	const char *end_ptr;
	StringInfoData str;

	fmt_str = expand_get_strval(PointerGetDatum(json), "fmt");
	if (fmt_str == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid NULL format string")));
	fmt_len = strlen(fmt_str);

	start_ptr = fmt_str;
	end_ptr = start_ptr + fmt_len;
	initStringInfo(&str);

	for (cp = start_ptr; cp < end_ptr; cp++)
	{
		convSpecifier specifier;
		bool		is_array;
		char	   *param = NULL;
		char	   *arraysep = NULL;
		Datum		paramval;
		char	   *paramtype;
		JsonType	json_elt_type;

		if (*cp != '%')
		{
			appendStringInfoCharMacro(&str, *cp);
			continue;
		}

		is_array = false;

		ADVANCE_PARSE_POINTER(cp, end_ptr);

		/* Easy case: %% outputs a single % */
		if (*cp == '%')
		{
			appendStringInfoCharMacro(&str, *cp);
			continue;
		}

		/*
		 * Scan the mandatory element name.  Allow for an array separator
		 * (which may be the empty string) to be specified after colon.
		 */
		if (*cp == '{')
		{
			StringInfoData parbuf;
			StringInfoData arraysepbuf;
			StringInfo	appendTo;

			initStringInfo(&parbuf);
			appendTo = &parbuf;

			ADVANCE_PARSE_POINTER(cp, end_ptr);
			for (; cp < end_ptr;)
			{
				if (*cp == ':')
				{
					/*
					 * found array separator delimiter; element name is now
					 * complete, start filling the separator.
					 */
					initStringInfo(&arraysepbuf);
					appendTo = &arraysepbuf;
					is_array = true;
					ADVANCE_PARSE_POINTER(cp, end_ptr);
					continue;
				}

				if (*cp == '}')
				{
					ADVANCE_PARSE_POINTER(cp, end_ptr);
					break;
				}
				appendStringInfoCharMacro(appendTo, *cp);
				ADVANCE_PARSE_POINTER(cp, end_ptr);
			}
			param = parbuf.data;
			if (is_array)
				arraysep = arraysepbuf.data;
		}
		if (param == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("missing conversion name in conversion specifier")));

		/*------
		 * The following conversion specifiers are currently recognized:
		 * 'I' -- expand as an identifier, adding quotes if necessary
		 * 'D' -- expand as a dotted-name, for qualified names; each element is
		 *		  quoted if necessary
		 * 'O' -- expand as an operator name. Same as 'D', but the objname
		 *		  element is not quoted.
		 * 's' -- expand as a simple string; no quoting.
		 * 'd' -- expand as a number.
		 * 'T' -- expand as a typename, with ad-hoc rules
		 */
		switch (*cp)
		{
			case 'I':
				specifier = SpecIdentifier;
				break;
			case 'D':
				specifier = SpecDottedName;
				break;
			case 's':
				specifier = SpecString;
				break;
			case 'd':
				specifier = SpecNumber;
				break;
			case 'L':
				specifier = SpecStringLiteral;
				break;
			case 'T':
				specifier = SpecTypename;
				break;
			case 'O':
				specifier = SpecOperatorname;
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid conversion specifier \"%c\"", *cp)));
		}

		/*
		 * Obtain the element to be expanded.  Note we cannot use
		 * DirectFunctionCall here, because the element might not exist.
		 */
		{
			FunctionCallInfoData fcinfo;

			InitFunctionCallInfoData(fcinfo, NULL, 2, InvalidOid, NULL, NULL);

			fcinfo.arg[0] = PointerGetDatum(json);
			fcinfo.argnull[0] = false;
			fcinfo.arg[1] = CStringGetTextDatum(param);
			fcinfo.argnull[1] = false;

			paramval = (*json_object_field) (&fcinfo);

			if (fcinfo.isnull)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("non-existant element \"%s\" in JSON formatting object",
								param)));
			}
		}

		/* figure out its type */
		json_elt_type = jsonval_get_type(paramval, &paramtype);

		/* Validate that we got an array if the format string specified one. */
		if (is_array && json_elt_type != JsonTypeArray)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("expected JSON array for element \"%s\", got %s",
							param, paramtype)));

		/* And finally print out the data */
		if (is_array)
		{
			int			count;
			bool		putsep = false;
			int			i;

			count = DatumGetInt32(DirectFunctionCall1(json_array_length,
													  paramval));
			for (i = 0; i < count; i++)
			{
				if (putsep)
					appendStringInfoString(&str, arraysep);
				putsep = true;

				expand_one_array_element(&str, paramval, i, param, specifier);
			}
		}
		else
		{
			expand_one_element(&str, param, paramval, paramtype, json_elt_type,
							   specifier);
		}
	}

	PG_RETURN_TEXT_P(CStringGetTextDatum(str.data));
}
