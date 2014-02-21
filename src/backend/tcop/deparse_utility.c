/*-------------------------------------------------------------------------
 *
 * deparse_utility.c
 *	  Functions to convert utility commands to machine-parseable
 *	  representation
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * NOTES
 *
 * This is intended to provide JSON blobs representing DDL commands, which can
 * later be re-processed into plain strings by well-defined sprintf-like
 * expansion.  These JSON objects are intended to allow for machine-editing of
 * the commands, by replacing certain nodes within the objects.
 *
 * Much of the information in the output blob actually comes from system
 * catalogs, not from the command parse node, as it is impossible to reliably
 * construct a fully-specified command (i.e. one not dependent on search_path
 * etc) looking only at the parse node.
 *
 * IDENTIFICATION
 *	  src/backend/tcop/deparse_utility.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_range.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "parser/analyze.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "rewrite/rewriteHandler.h"
#include "tcop/deparse_utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"


typedef enum
{
	ObjTypeNull,
	ObjTypeBool,
	ObjTypeString,
	ObjTypeArray,
	ObjTypeObject
} ObjType;

typedef struct ObjElem
{
	char	   *name;
	ObjType		objtype;
	bool		bool_value;
	char	   *str_value;
	struct ObjTree *obj_value;
	List	   *array_value;
	slist_node	node;
} ObjElem;

typedef struct ObjTree
{
	MemoryContext cxt;
	slist_head	params;
	int			numParams;
} ObjTree;


static ObjElem *new_null_object(ObjTree *tree, char *name);
static ObjElem *new_bool_object(ObjTree *tree, char *name, bool value);
static ObjElem *new_string_object(ObjTree *tree, char *name, char *value);
static ObjElem *new_object_object(ObjTree *tree, char *name, ObjTree *value);
static ObjElem *new_array_object(ObjTree *tree, char *name, List *array);
static void append_null_object(ObjTree *tree, char *name);
static void append_bool_object(ObjTree *tree, char *name,
				   bool value);
static void append_string_object(ObjTree *tree, char *name,
					 char *value);
static void append_object_object(ObjTree *tree, char *name,
					 ObjTree *value);
static void append_array_object(ObjTree *tree, char *name,
					List *array);
static inline void append_premade_object(ObjTree *tree, ObjElem *elem);

/*
 * Allocate a new object tree to store parameter values.  If parent is NULL, a
 * new memory context is created for all allocations involving the parameters;
 * if it's not null, then the memory context from the given object is used.
 */
static ObjTree *
new_objtree(ObjTree *parent)
{
	MemoryContext cxt;
	ObjTree    *params;

	if (parent == NULL)
	{
		cxt = AllocSetContextCreate(CurrentMemoryContext,
									"deparse parameters",
									ALLOCSET_DEFAULT_MINSIZE,
									ALLOCSET_DEFAULT_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);
	}
	else
		cxt = parent->cxt;

	params = MemoryContextAlloc(cxt, sizeof(ObjTree));
	params->cxt = cxt;
	params->numParams = 0;
	slist_init(&params->params);

	return params;
}

/*
 * Allocate a new object to store parameters.  As above, if parent is NULL,
 * a new memory context is created; otherwise the parent is used to extract
 * the memory context to use.
 *
 * The "fmt" argument is used to append as a "fmt" element in the output blob.
 * numobjs indicates the number of extra elements to append; for each one,
 * a name, type and value must be supplied.  Note we don't have the luxury of
 * sprintf-like compiler warnings for malformed argument lists.
 */
static ObjTree *
new_objtree_VA(ObjTree *parent, char *fmt, int numobjs,...)
{
	ObjTree    *tree;
	va_list		args;
	int			i;

	/* Set up the toplevel object and its "fmt" */
	tree = new_objtree(parent);
	append_string_object(tree, "fmt", fmt);

	/* And process the given varargs */
	va_start(args, numobjs);
	for (i = 0; i < numobjs; i++)
	{
		ObjTree    *value;
		ObjType		type;
		ObjElem	   *elem;
		char	   *name;
		char	   *strval;
		bool		boolval;
		List	   *list;

		name = va_arg(args, char *);
		type = va_arg(args, ObjType);

		/* Null params don't have a value (obviously) */
		if (type == ObjTypeNull)
		{
			append_null_object(tree, name);
			continue;
		}

		/*
		 * For all other param types there must be a value in the varargs.
		 * Fetch it and add the fully formed subobject into the main object.
		 */
		switch (type)
		{
			case ObjTypeBool:
				boolval = va_arg(args, int);
				elem = new_bool_object(tree, name, boolval);
				break;
			case ObjTypeString:
				strval = va_arg(args, char *);
				elem = new_string_object(tree, name, strval);
				break;
			case ObjTypeObject:
				value = va_arg(args, ObjTree *);
				elem = new_object_object(tree, name, value);
				break;
			case ObjTypeArray:
				list = va_arg(args, List *);
				elem = new_array_object(tree, name, list);
				break;
			default:
				elog(ERROR, "invalid parameter type %d", type);
		}

		append_premade_object(tree, elem);
	}

	va_end(args);
	return tree;
}

/*
 * Add a new parameter with a NULL value
 */
static ObjElem *
new_null_object(ObjTree *tree, char *name)
{
	ObjElem    *param;

	param = MemoryContextAllocZero(tree->cxt, sizeof(ObjElem));

	param->name = name ? MemoryContextStrdup(tree->cxt, name) : NULL;
	param->objtype = ObjTypeNull;

	return param;
}

static void
append_null_object(ObjTree *tree, char *name)
{
	ObjElem    *param;

	param = new_null_object(tree, name);
	append_premade_object(tree, param);
}

/*
 * Add a new boolean parameter
 */
static ObjElem *
new_bool_object(ObjTree *tree, char *name, bool value)
{
	ObjElem    *param;

	param = MemoryContextAllocZero(tree->cxt, sizeof(ObjElem));

	param->name = name ? MemoryContextStrdup(tree->cxt, name) : NULL;
	param->objtype = ObjTypeBool;
	param->bool_value = value;

	return param;
}

static void
append_bool_object(ObjTree *tree, char *name, bool value)
{
	ObjElem    *param;

	param = new_bool_object(tree, name, value);
	append_premade_object(tree, param);
}

static ObjElem *
new_string_object(ObjTree *tree, char *name, char *value)
{
	ObjElem    *param;

	param = MemoryContextAllocZero(tree->cxt, sizeof(ObjElem));

	param->name = name ? MemoryContextStrdup(tree->cxt, name) : NULL;
	param->objtype = ObjTypeString;
	param->str_value = value;	/* XXX not duped */

	return param;
}

/*
 * Add a new string parameter.
 *
 * Note: we don't pstrdup the source string.  Caller must ensure the
 * source string lives long enough.
 */
static void
append_string_object(ObjTree *tree, char *name, char *value)
{
	ObjElem	   *param;

	param = new_string_object(tree, name, value);
	append_premade_object(tree, param);
}

/*
 * Add a new object parameter
 */
static ObjElem *
new_object_object(ObjTree *tree, char *name, ObjTree *value)
{
	ObjElem    *param;

	param = MemoryContextAllocZero(tree->cxt, sizeof(ObjElem));

	param->name = name ? MemoryContextStrdup(tree->cxt, name) : NULL;
	param->objtype = ObjTypeObject;
	param->obj_value = value;	/* XXX not duped */

	return param;
}

static void
append_object_object(ObjTree *tree, char *name, ObjTree *value)
{
	ObjElem    *param;

	param = new_object_object(tree, name, value);
	append_premade_object(tree, param);
}

/*
 * Add a new array parameter
 */
static ObjElem *
new_array_object(ObjTree *tree, char *name, List *array)
{
	ObjElem    *param;

	param = MemoryContextAllocZero(tree->cxt, sizeof(ObjElem));

	param->name = name ? MemoryContextStrdup(tree->cxt, name) : NULL;
	param->objtype = ObjTypeArray;
	param->array_value = array; /* XXX not duped */

	return param;
}

static void
append_array_object(ObjTree *tree, char *name, List *array)
{
	ObjElem    *param;

	param = new_array_object(tree, name, array);
	append_premade_object(tree, param);
}

static inline void
append_premade_object(ObjTree *tree, ObjElem *elem)
{
	slist_push_head(&tree->params, &elem->node);
	tree->numParams++;
}

/*
 * Create a JSON blob from our ad-hoc representation.
 *
 * Note this allocates memory in tree->cxt.  This is okay because we don't need
 * a separate memory context, and the one provided by the tree object has the
 * right lifetime already.
 *
 * XXX this implementation will fail if there are more JSON objects in the tree
 * than the maximum number of columns in a heap tuple.  To fix we would first call
 * construct_md_array and then json_object.
 */
static char *
jsonize_objtree(ObjTree *tree)
{
	MemoryContext oldcxt;
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *nulls;
	slist_iter	iter;
	int			i;
	HeapTuple	htup;
	Datum		json;
	char	   *jsonstr;

	/*
	 * Use the objtree's memory context, so that everything we allocate in
	 * this routine (other than our return string) can be blown up easily
	 * by our caller.
	 */
	oldcxt = MemoryContextSwitchTo(tree->cxt);
	tupdesc = CreateTemplateTupleDesc(tree->numParams, false);
	values = palloc(sizeof(Datum) * tree->numParams);
	nulls = palloc(sizeof(bool) * tree->numParams);

	i = 1;
	slist_foreach(iter, &tree->params)
	{
		ObjElem    *object = slist_container(ObjElem, node, iter.cur);
		Oid			typeid;

		switch (object->objtype)
		{
			case ObjTypeNull:
			case ObjTypeString:
				typeid = TEXTOID;
				break;
			case ObjTypeBool:
				typeid = BOOLOID;
				break;
			case ObjTypeArray:
			case ObjTypeObject:
				typeid = JSONOID;
				break;
			default:
				elog(ERROR, "unable to determine type id");
				typeid = InvalidOid;
		}

		TupleDescInitEntry(tupdesc, i, object->name, typeid, -1, 0);

		nulls[i - 1] = false;
		switch (object->objtype)
		{
			case ObjTypeNull:
				nulls[i - 1] = true;
				break;
			case ObjTypeBool:
				values[i - 1] = BoolGetDatum(object->bool_value);
				break;
			case ObjTypeString:
				values[i - 1] = CStringGetTextDatum(object->str_value);
				break;
			case ObjTypeArray:
				{
					ArrayType  *arrayt;
					Datum	   *arrvals;
					Datum		jsonary;
					ListCell   *cell;
					int			length = list_length(object->array_value);
					int			j;

					/*
					 * Arrays are stored as Lists up to this point, with each
					 * element being a ObjElem; we need to construct an
					 * ArrayType with them to turn the whole thing into a JSON
					 * array.
					 */
					j = 0;
					arrvals = palloc(sizeof(Datum) * length);
					foreach(cell, object->array_value)
					{
						ObjElem    *elem = lfirst(cell);

						switch (elem->objtype)
						{
							case ObjTypeString:
								arrvals[j] =
									/*
									 * XXX need quotes around the value.  This
									 * needs to be handled differently because
									 * it will fail for values of anything but
									 * trivial complexity.
									 */
									CStringGetTextDatum(psprintf("\"%s\"",
																 elem->str_value));
								break;
							case ObjTypeObject:
								arrvals[j] =
									CStringGetTextDatum(jsonize_objtree(elem->obj_value));
								break;
							default:
								/* not worth supporting other cases */
								elog(ERROR, "unsupported object type %d",
									 elem->objtype);
						}

						j++;

					}
					arrayt = construct_array(arrvals, length,
											 JSONOID, -1, false, 'i');

					jsonary = DirectFunctionCall1(array_to_json,
												  (PointerGetDatum(arrayt)));

					values[i - 1] = jsonary;
				}
				break;
			case ObjTypeObject:
				values[i - 1] =
					CStringGetTextDatum(jsonize_objtree(object->obj_value));
				break;
		}

		i++;
	}

	BlessTupleDesc(tupdesc);
	htup = heap_form_tuple(tupdesc, values, nulls);
	json = DirectFunctionCall1(row_to_json, HeapTupleGetDatum(htup));

	/* switch to caller's context so that our output is allocated there */
	MemoryContextSwitchTo(oldcxt);

	jsonstr = TextDatumGetCString(json);

	return jsonstr;
}

/*
 * Release all memory used by parameters and their expansion
 */
static void
free_objtree(ObjTree *tree)
{
	MemoryContextDelete(tree->cxt);
}

/*
 * A helper routine to setup %{}T elements.
 */
static ObjTree *
new_objtree_for_type(ObjTree *parent, Oid typeId, int32 typmod)
{
	ObjTree    *typeParam;
	Oid			typnspid;
	char	   *typnsp;
	char	   *typename;
	char	   *typmodstr;
	bool		is_array;

	format_type_detailed(typeId, typmod,
						 &typnspid, &typename, &typmodstr, &is_array);

	if (isAnyTempNamespace(typnspid))
		typnsp = pstrdup("pg_temp");
	else
		typnsp = get_namespace_name(typnspid);

	/*
	 * XXX We need this kludge to support types whose typmods include extra
	 * verbiage after the parenthised value.  Really, this only applies to
	 * timestamp and timestamptz, whose the typmod takes the form "(N)
	 * with[out] time zone", which causes a syntax error with schema-qualified
	 * names extracted from pg_type (as opposed to specialized type names
	 * defined by the SQL standard).
	 */
	if (typmodstr)
	{
		char	*clpar;

		clpar = strchr(typmodstr, ')');
		if (clpar)
			*(clpar + 1) = '\0';
	}

	/* We don't use new_objtree_VA here because types don't have a "fmt" */
	typeParam = new_objtree(parent);
	append_string_object(typeParam, "schemaname", typnsp);
	append_string_object(typeParam, "typename", typename);
	append_string_object(typeParam, "typmod", typmodstr);
	append_bool_object(typeParam, "is_array", is_array);

	return typeParam;
}

/*
 * A helper routine to setup %{}D and %{}O elements
 *
 * Elements "schemaname" and "objname" are set.  If the namespace OID
 * corresponds to a temp schema, that's set to "pg_temp".
 *
 * The difference between those two element types is whether the objname will
 * be quoted as an identifier or not, which is not something that this routine
 * concerns itself with; that will be up to the expand function.
 */
static ObjTree *
new_objtree_for_qualname(ObjTree *parent, Oid nspid, char *name)
{
	ObjTree    *qualified;
	char	   *namespace;

	/*
	 * We don't use new_objtree_VA here because these names don't have a "fmt"
	 */
	qualified = new_objtree(parent);
	if (isAnyTempNamespace(nspid))
		namespace = pstrdup("pg_temp");
	else
		namespace = get_namespace_name(nspid);
	append_string_object(qualified, "schemaname", namespace);
	append_string_object(qualified, "objname", pstrdup(name));

	return qualified;
}

/*
 * A helper routine to setup %{}D and %{}O elements, with the object specified
 * by classId/objId
 *
 * Elements "schemaname" and "objname" are set.  If the object is a temporary
 * object, the schema name is set to "pg_temp".
 */
static ObjTree *
new_objtree_for_qualname_id(ObjTree *parent, Oid classId, Oid objectId)
{
	ObjTree    *qualified;
	Relation	catalog;
	HeapTuple	catobj;
	Datum		objnsp;
	Datum		objname;
	AttrNumber	Anum_name;
	AttrNumber	Anum_namespace;
	bool		isnull;

	catalog = heap_open(classId, AccessShareLock);

	catobj = get_catalog_object_by_oid(catalog, objectId);
	if (!catobj)
		elog(ERROR, "cache lookup failed for object %u of catalog \"%s\"",
			 objectId, RelationGetRelationName(catalog));
	Anum_name = get_object_attnum_name(classId);
	Anum_namespace = get_object_attnum_namespace(classId);

	objnsp = heap_getattr(catobj, Anum_namespace, RelationGetDescr(catalog),
						  &isnull);
	if (isnull)
		elog(ERROR, "unexpected NULL namespace");
	objname = heap_getattr(catobj, Anum_name, RelationGetDescr(catalog),
						   &isnull);
	if (isnull)
		elog(ERROR, "unexpected NULL name");

	qualified = new_objtree_for_qualname(parent,
										 DatumGetObjectId(objnsp),
										 NameStr(*DatumGetName(objname)));

	pfree(catobj);
	heap_close(catalog, AccessShareLock);

	return qualified;
}

/*
 * Return the string representation of the given RELPERSISTENCE value
 */
static char *
get_persistence_str(char persistence)
{
	switch (persistence)
	{
		case RELPERSISTENCE_TEMP:
			return "TEMPORARY";
		case RELPERSISTENCE_UNLOGGED:
			return "UNLOGGED";
		case RELPERSISTENCE_PERMANENT:
			return "";
		default:
			return "???";
	}
}

/*
 * deparse_ViewStmt
 *		deparse a ViewStmt
 *
 * Given a view OID and the parsetree that created it, return the JSON blob
 * representing the creation command.
 */
static char *
deparse_ViewStmt(Oid objectId, Node *parsetree)
{
	ObjTree    *viewStmt;
	ObjTree    *tmp;
	char	   *command;
	Relation	relation;

	relation = relation_open(objectId, AccessShareLock);

	viewStmt = new_objtree_VA(NULL,
					 "CREATE %{persistence}s VIEW %{identity}D AS %{query}s",
							  1, "persistence", ObjTypeString,
					  get_persistence_str(relation->rd_rel->relpersistence));

	tmp = new_objtree_for_qualname(viewStmt,
								   relation->rd_rel->relnamespace,
								   RelationGetRelationName(relation));
	append_object_object(viewStmt, "identity", tmp);

	append_string_object(viewStmt, "query",
						 pg_get_viewdef_internal(objectId));

	command = jsonize_objtree(viewStmt);
	free_objtree(viewStmt);

	relation_close(relation, AccessShareLock);

	return command;
}

/*
 * deparse_CreateTrigStmt
 *		Deparse a CreateTrigStmt (CREATE TRIGGER)
 *
 * Given a trigger OID and the parsetree that created it, return the JSON blob
 * representing the creation command.
 */
static char *
deparse_CreateTrigStmt(Oid objectId, Node *parsetree)
{
	CreateTrigStmt *node = (CreateTrigStmt *) parsetree;
	Relation	pg_trigger;
	HeapTuple	trigTup;
	Form_pg_trigger trigForm;
	ObjTree	   *trigger;
	ObjTree	   *tmp;
	int			tgnargs;
	List	   *list;
	List	   *events;
	char	   *command;

	pg_trigger = heap_open(TriggerRelationId, AccessShareLock);

	trigTup = get_catalog_object_by_oid(pg_trigger, objectId);
	trigForm = (Form_pg_trigger) GETSTRUCT(trigTup);

	/*
	 * Some of the elements only make sense for CONSTRAINT TRIGGERs, but it
	 * seems simpler to use a single fmt string for both kinds of triggers.
	 */
	trigger =
		new_objtree_VA(NULL,
					   "CREATE %{constraint}s TRIGGER %{name}I %{time}s %{events: OR }s "
					   "ON %{relation}D %{from_table}s %{constraint_attrs: }s "
					   "FOR EACH %{for_each}s %{when}s EXECUTE PROCEDURE %{function}s",
					   2,
					   "name", ObjTypeString, node->trigname,
					   "constraint", ObjTypeString,
					   node->isconstraint ? "CONSTRAINT" : "");

	if (node->timing == TRIGGER_TYPE_BEFORE)
		append_string_object(trigger, "time", "BEFORE");
	else if (node->timing == TRIGGER_TYPE_AFTER)
		append_string_object(trigger, "time", "AFTER");
	else if (node->timing == TRIGGER_TYPE_INSERT)
		append_string_object(trigger, "time", "INSTEAD OF");
	else
		elog(ERROR, "unrecognized trigger timing value %d", node->timing);

	/*
	 * Decode the events that the trigger fires for.  The output is a list;
	 * in most cases it will just be a string with the even name, but when
	 * there's an UPDATE with a list of columns, we return a JSON object.
	 */
	events = NIL;
	if (node->events & TRIGGER_TYPE_INSERT)
		events = lappend(events,
						 new_string_object(trigger, NULL, "INSERT"));
	if (node->events & TRIGGER_TYPE_DELETE)
		events = lappend(events,
						 new_string_object(trigger, NULL, "DELETE"));
	if (node->events & TRIGGER_TYPE_TRUNCATE)
		events = lappend(events,
						 new_string_object(trigger, NULL, "TRUNCATE"));
	if (node->events & TRIGGER_TYPE_UPDATE)
	{
		if (node->columns == NIL)
		{
			events = lappend(events,
							 new_string_object(trigger, NULL, "UPDATE"));
		}
		else
		{
			ObjTree	   *update;
			ListCell   *cell;
			List	   *cols = NIL;

			/*
			 * Currently only UPDATE OF can be objects in the output JSON, but
			 * we add a "kind" element so that user code can distinguish
			 * possible future new event types.
			 */
			update = new_objtree_VA(trigger, "UPDATE OF %{columns:, }I",
									1, "kind", ObjTypeString, "update_of");

			foreach(cell, node->columns)
			{
				char   *colname = strVal(lfirst(cell));

				cols = lappend(cols,
							   new_string_object(trigger, NULL, colname));
			}

			append_array_object(update, "columns", cols);

			events = lappend(events,
							 new_object_object(trigger, NULL, update));
		}
	}
	append_array_object(trigger, "events", events);

	tmp = new_objtree_for_qualname_id(trigger,
									  RelationRelationId,
									  trigForm->tgrelid);
	append_object_object(trigger, "relation", tmp);

	tmp = new_objtree_VA(trigger, "FROM %{relation}D", 0);
	if (trigForm->tgconstrrelid)
	{
		ObjTree	   *rel;

		rel = new_objtree_for_qualname_id(trigger,
										  RelationRelationId,
										  trigForm->tgconstrrelid);
		append_object_object(tmp, "relation", rel);
	}
	else
		append_bool_object(tmp, "present", false);
	append_object_object(trigger, "from_table", tmp);

	list = NIL;
	if (node->deferrable)
		list = lappend(list,
					   new_string_object(trigger, NULL, "DEFERRABLE"));
	if (node->initdeferred)
		list = lappend(list,
					   new_string_object(trigger, NULL, "INITIALLY DEFERRED"));
	append_array_object(trigger, "constraint_attrs", list);

	append_string_object(trigger, "for_each",
						 node->row ? "ROW" : "STATEMENT");

	tmp = new_objtree_VA(trigger, "WHEN %{clause}s", 0);
	if (node->whenClause)
	{
		Node	   *whenClause;
		Datum		value;
		bool		isnull;

		value = fastgetattr(trigTup, Anum_pg_trigger_tgqual,
							RelationGetDescr(pg_trigger), &isnull);
		if (isnull)
			elog(ERROR, "bogus NULL tgqual");

		whenClause = stringToNode(TextDatumGetCString(value));
		append_string_object(tmp, "clause",
							 pg_get_trigger_whenclause(trigForm,
													   whenClause,
													   false));
	}
	else
		append_bool_object(tmp, "present", false);
	append_object_object(trigger, "when", tmp);

	tmp = new_objtree_VA(trigger, "%{funcname}D(%{args:, }L)",
						 1, "funcname", ObjTypeObject,
						 new_objtree_for_qualname_id(trigger,
													 ProcedureRelationId,
													 trigForm->tgfoid));
	list = NIL;
	tgnargs = trigForm->tgnargs;
	if (tgnargs > 0)
	{
		bytea  *tgargs;
		char   *argstr;
		bool	isnull;
		int		findx;
		int		lentgargs;
		char   *p;

		tgargs = DatumGetByteaP(fastgetattr(trigTup,
											Anum_pg_trigger_tgargs,
											RelationGetDescr(pg_trigger),
											&isnull));
		if (isnull)
			elog(ERROR, "invalid NULL tgargs");
		argstr = (char *) VARDATA(tgargs);
		lentgargs = VARSIZE_ANY_EXHDR(tgargs);

		p = argstr;
		for (findx = 0; findx < tgnargs; findx++)
		{
			size_t	tlen;

			/* verify that the argument encoding is correct */
			tlen = strlen(p);
			if (p + tlen >= argstr + lentgargs)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid argument string (%s) for trigger \"%s\"",
								argstr, NameStr(trigForm->tgname))));

			list = lappend(list, new_string_object(trigger, NULL, p));

			p += tlen + 1;
		}
	}

	append_array_object(tmp, "args", list);		/* might be NIL */

	append_object_object(trigger, "function", tmp);

	heap_close(pg_trigger, AccessShareLock);

	command = jsonize_objtree(trigger);
	free_objtree(trigger);

	return command;
}

/*
 * deparse_ColumnDef
 *		Subroutine for CREATE TABLE deparsing
 *
 * Deparse a ColumnDef node within a regular (non typed) table creation.
 *
 * NOT NULL constraints in the column definition are emitted directly in the
 * column definition by this routine; other constraints must be emitted
 * elsewhere (the info in the parse node is incomplete anyway.)
 */
static ObjElem *
deparse_ColumnDef(ObjTree *parent, Relation relation, List *dpcontext,
				  bool composite, ColumnDef *coldef)
{
	ObjTree    *column;
	ObjTree    *tmp;
	Oid			relid = RelationGetRelid(relation);
	HeapTuple	attrTup;
	Form_pg_attribute attrForm;
	Oid			typid;
	int32		typmod;
	Oid			typcollation;
	bool		saw_notnull;
	ListCell   *cell;

	/*
	 * Inherited columns without local definitions must not be emitted. XXX --
	 * maybe it is useful to have them with "present = false" or some such?
	 */
	if (!coldef->is_local)
		return NULL;

	attrTup = SearchSysCacheAttName(relid, coldef->colname);
	if (!HeapTupleIsValid(attrTup))
		elog(ERROR, "could not find cache entry for column \"%s\" of relation %u",
			 coldef->colname, relid);
	attrForm = (Form_pg_attribute) GETSTRUCT(attrTup);

	get_atttypetypmodcoll(relid, attrForm->attnum,
						  &typid, &typmod, &typcollation);

	/* Composite types use a slightly simpler format string */
	if (composite)
		column = new_objtree_VA(parent,
								"%{name}I %{coltype}T %{collation}s",
								3,
								"type", ObjTypeString, "column",
								"name", ObjTypeString, coldef->colname,
								"coltype", ObjTypeObject,
								new_objtree_for_type(parent, typid, typmod));
	else
		column = new_objtree_VA(parent,
								"%{name}I %{coltype}T %{default}s %{not_null}s %{collation}s",
								3,
								"type", ObjTypeString, "column",
								"name", ObjTypeString, coldef->colname,
								"coltype", ObjTypeObject,
								new_objtree_for_type(parent, typid, typmod));

	tmp = new_objtree_VA(parent, "COLLATE %{name}D", 0);
	if (OidIsValid(typcollation))
	{
		ObjTree *collname;

		collname = new_objtree_for_qualname_id(parent,
											   CollationRelationId,
											   typcollation);
		append_object_object(tmp, "name", collname);
	}
	else
		append_bool_object(tmp, "present", false);
	append_object_object(column, "collation", tmp);

	if (!composite)
	{
		/*
		 * Emit a NOT NULL declaration if necessary.  Note that we cannot trust
		 * pg_attribute.attnotnull here, because that bit is also set when
		 * primary keys are specified; and we must not emit a NOT NULL
		 * constraint in that case, unless explicitely specified.  Therefore,
		 * we scan the list of constraints attached to this column to determine
		 * whether we need to emit anything.
		 * (Fortunately, NOT NULL constraints cannot be table constraints.)
		 */
		saw_notnull = false;
		foreach(cell, coldef->constraints)
		{
			Constraint *constr = (Constraint *) lfirst(cell);

			if (constr->contype == CONSTR_NOTNULL)
				saw_notnull = true;
		}

		if (saw_notnull)
			append_string_object(column, "not_null", "NOT NULL");
		else
			append_string_object(column, "not_null", "");

		tmp = new_objtree_VA(parent, "DEFAULT %{default}s", 0);
		if (attrForm->atthasdef)
		{
			char *defstr;

			defstr = RelationGetColumnDefault(relation, attrForm->attnum,
											  dpcontext);

			append_string_object(tmp, "default", defstr);
		}
		else
			append_bool_object(tmp, "present", false);
		append_object_object(column, "default", tmp);
	}

	ReleaseSysCache(attrTup);

	return new_object_object(parent, NULL, column);
}

/*
 * deparse_ColumnDef_Typed
 *		Subroutine for CREATE TABLE OF deparsing
 *
 * Deparse a ColumnDef node within a typed table creation.	This is simpler
 * than the regular case, because we don't have to emit the type declaration,
 * collation, or default.  Here we only return something if the column is being
 * declared NOT NULL.
 *
 * As in deparse_ColumnDef, any other constraint is processed elsewhere.
 *
 * FIXME --- actually, what about default values?
 */
static ObjElem *
deparse_ColumnDef_typed(ObjTree *parent, Relation relation, List *dpcontext,
						ColumnDef *coldef)
{
	ObjTree    *column = NULL;
	Oid			relid = RelationGetRelid(relation);
	HeapTuple	attrTup;
	Form_pg_attribute attrForm;
	Oid			typid;
	int32		typmod;
	Oid			typcollation;
	bool		saw_notnull;
	ListCell   *cell;

	attrTup = SearchSysCacheAttName(relid, coldef->colname);
	if (!HeapTupleIsValid(attrTup))
		elog(ERROR, "could not find cache entry for column \"%s\" of relation %u",
			 coldef->colname, relid);
	attrForm = (Form_pg_attribute) GETSTRUCT(attrTup);

	get_atttypetypmodcoll(relid, attrForm->attnum,
						  &typid, &typmod, &typcollation);

	/*
	 * Search for a NOT NULL declaration.  As in deparse_ColumnDef, we rely on
	 * finding a constraint on the column rather than coldef->is_not_null.
	 */
	saw_notnull = false;
	foreach(cell, coldef->constraints)
	{
		Constraint *constr = (Constraint *) lfirst(cell);

		if (constr->contype == CONSTR_NOTNULL)
		{
			saw_notnull = true;
			break;
		}
	}

	if (saw_notnull)
		column = new_objtree_VA(parent,
								"%{name}I WITH OPTIONS NOT NULL", 2,
								"type", ObjTypeString, "column_notnull",
								"name", ObjTypeString, coldef->colname);

	ReleaseSysCache(attrTup);
	return new_object_object(parent, NULL, column);
}

/*
 * deparseTableElements
 *		Subroutine for CREATE TABLE deparsing
 *
 * Deal with all the table elements (columns and constraints).
 *
 * Note we ignore constraints in the parse node here; they are extracted from
 * system catalogs instead.
 */
static List *
deparseTableElements(List *elements, ObjTree *parent, Relation relation,
					 List *tableElements, List *dpcontext, bool typed,
					 bool composite)
{
	ListCell   *lc;

	foreach(lc, tableElements)
	{
		Node	   *elt = (Node *) lfirst(lc);

		switch (nodeTag(elt))
		{
			case T_ColumnDef:
				{
					ObjElem    *column;

					column = typed ?
						deparse_ColumnDef_typed(parent, relation, dpcontext,
												(ColumnDef *) elt) :
						deparse_ColumnDef(parent, relation, dpcontext,
										  composite, (ColumnDef *) elt);

					if (column != NULL)
						elements = lappend(elements, column);
				}
				break;
			case T_Constraint:
				break;
			default:
				elog(ERROR, "invalid node type %d", nodeTag(elt));
		}
	}

	return elements;
}

/*
 * obtainTableConstraints
 *		Subroutine for CREATE TABLE deparsing
 *
 * Given a table OID, obtain its constraints and append them to the given
 * elements list.  The updated list is returned.
 *
 * This works for both typed and regular tables.
 */
static List *
obtainTableConstraints(List *elements, Oid objectId, ObjTree *parent)
{
	Relation	conRel;
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	tuple;
	ObjTree    *tmp;

	/*
	 * scan pg_constraint to fetch all constraints linked to the given
	 * relation.
	 */
	conRel = heap_open(ConstraintRelationId, AccessShareLock);
	ScanKeyInit(&key,
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objectId));
	scan = systable_beginscan(conRel, ConstraintRelidIndexId,
							  true, NULL, 1, &key);

	/*
	 * For each constraint, add a node to the list of table elements.  In
	 * these nodes we include not only the printable information ("fmt"), but
	 * also separate attributes to indicate the type of constraint, for
	 * automatic processing.
	 */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_constraint constrForm;
		char	   *contype;

		constrForm = (Form_pg_constraint) GETSTRUCT(tuple);

		switch (constrForm->contype)
		{
			case CONSTRAINT_CHECK:
				contype = "check";
				break;
			case CONSTRAINT_FOREIGN:
				contype = "foreign key";
				break;
			case CONSTRAINT_PRIMARY:
				contype = "primary key";
				break;
			case CONSTRAINT_UNIQUE:
				contype = "unique";
				break;
			case CONSTRAINT_TRIGGER:
				contype = "trigger";
				break;
			case CONSTRAINT_EXCLUSION:
				contype = "exclusion";
				break;
			default:
				elog(ERROR, "unrecognized constraint type");
		}

		/*
		 * "type" and "contype" are not part of the printable output, but are
		 * useful to programmatically distinguish these from columns and among
		 * different constraint types.
		 *
		 * XXX it might be useful to also list the column names in a PK, etc.
		 */
		tmp = new_objtree_VA(parent,
							 "CONSTRAINT %{name}I %{definition}s",
							 4,
							 "type", ObjTypeString, "constraint",
							 "contype", ObjTypeString, contype,
						 "name", ObjTypeString, NameStr(constrForm->conname),
							 "definition", ObjTypeString,
						  pg_get_constraintdef_string(HeapTupleGetOid(tuple),
													  false));
		elements = lappend(elements,
						   new_object_object(parent, NULL, tmp));
	}

	systable_endscan(scan);
	heap_close(conRel, AccessShareLock);

	return elements;
}

/*
 * deparse_CreateStmt
 *		Deparse a CreateStmt (CREATE TABLE)
 *
 * Given a table OID and the parsetree that created it, return the JSON blob
 * representing the creation command.
 */
static char *
deparse_CreateStmt(Oid objectId, Node *parsetree)
{
	CreateStmt *node = (CreateStmt *) parsetree;
	Relation	relation = relation_open(objectId, AccessShareLock);
	List	   *dpcontext;
	ObjTree    *createStmt;
	ObjTree    *tmp;
	char	   *command;
	char	   *fmtstr;

	/*
	 * Typed tables use a slightly different format string: we must not put
	 * table_elements with parents directly in the fmt string, because if
	 * there are no options the parens must not be emitted; and also, typed
	 * tables do not allow for inheritance.
	 */
	if (node->ofTypename)
		fmtstr = "CREATE %{persistence}s TABLE %{if_not_exists}s %{identity}D "
			"OF %{of_type}T %{table_elements}s "
			"%{on_commit}s %{tablespace}s";
	else
		fmtstr = "CREATE %{persistence}s TABLE %{if_not_exists}s %{identity}D "
			"(%{table_elements:, }s) %{inherits}s "
			"%{on_commit}s %{tablespace}s";

	createStmt =
		new_objtree_VA(NULL, fmtstr, 1,
					   "persistence", ObjTypeString,
					   get_persistence_str(relation->rd_rel->relpersistence));

	tmp = new_objtree_for_qualname(createStmt,
								   relation->rd_rel->relnamespace,
								   RelationGetRelationName(relation));
	append_object_object(createStmt, "identity", tmp);

	append_string_object(createStmt, "if_not_exists",
						 node->if_not_exists ? "IF NOT EXISTS" : "");

	dpcontext = deparse_context_for(RelationGetRelationName(relation),
								  objectId);

	if (node->ofTypename)
	{
		List	   *tableelts = NIL;

		/*
		 * We can't put table elements directly in the fmt string as an array
		 * surrounded by parens here, because an empty clause would cause a
		 * syntax error.  Therefore, we use an indirection element and set
		 * present=false when there are no elements.
		 */
		append_string_object(createStmt, "table_kind", "typed");

		tmp = new_objtree_for_type(createStmt, relation->rd_rel->reloftype, -1);
		append_object_object(createStmt, "of_type", tmp);

		tableelts = deparseTableElements(NIL, createStmt, relation,
										 node->tableElts, dpcontext,
										 true,		/* typed table */
										 false);	/* not composite */
		tableelts = obtainTableConstraints(tableelts, objectId, createStmt);
		if (tableelts == NIL)
			tmp = new_objtree_VA(createStmt, "", 1,
								 "present", ObjTypeBool, false);
		else
			tmp = new_objtree_VA(createStmt, "(%{elements:, }s)", 1,
								 "elements", ObjTypeArray, tableelts);
		append_object_object(createStmt, "table_elements", tmp);
	}
	else
	{
		List	   *tableelts = NIL;

		/*
		 * There is no need to process LIKE clauses separately; they have
		 * already been transformed into columns and constraints.
		 */
		append_string_object(createStmt, "table_kind", "plain");

		/*
		 * Process table elements: column definitions and constraints.	Only
		 * the column definitions are obtained from the parse node itself.	To
		 * get constraints we rely on pg_constraint, because the parse node
		 * might be missing some things such as the name of the constraints.
		 */
		tableelts = deparseTableElements(NIL, createStmt, relation,
										 node->tableElts, dpcontext,
										 false,		/* not typed table */
										 false);	/* not composite */
		tableelts = obtainTableConstraints(tableelts, objectId, createStmt);

		append_array_object(createStmt, "table_elements", tableelts);

		/*
		 * Add inheritance specification.  We cannot simply scan the list of
		 * parents from the parser node, because that may lack the actual
		 * qualified names of the parent relations.  Rather than trying to
		 * re-resolve them from the information in the parse node, it seems
		 * more accurate and convenient to grab it from pg_inherits.
		 */
		tmp = new_objtree_VA(createStmt, "INHERITS (%{parents:, }D)", 0);
		if (list_length(node->inhRelations) > 0)
		{
			List	   *parents = NIL;
			Relation	inhRel;
			SysScanDesc scan;
			ScanKeyData key;
			HeapTuple	tuple;

			inhRel = heap_open(InheritsRelationId, RowExclusiveLock);

			ScanKeyInit(&key,
						Anum_pg_inherits_inhrelid,
						BTEqualStrategyNumber, F_OIDEQ,
						ObjectIdGetDatum(objectId));

			scan = systable_beginscan(inhRel, InheritsRelidSeqnoIndexId,
									  true, NULL, 1, &key);

			while (HeapTupleIsValid(tuple = systable_getnext(scan)))
			{
				ObjTree    *parent;
				Form_pg_inherits formInh = (Form_pg_inherits) GETSTRUCT(tuple);

				parent = new_objtree_for_qualname_id(createStmt,
													 RelationRelationId,
													 formInh->inhparent);

				parents = lappend(parents,
								  new_object_object(createStmt, NULL, parent));
			}

			systable_endscan(scan);
			heap_close(inhRel, RowExclusiveLock);

			append_array_object(tmp, "parents", parents);
		}
		else
		{
			append_null_object(tmp, "parents");
			append_bool_object(tmp, "present", false);
		}
		append_object_object(createStmt, "inherits", tmp);
	}

	tmp = new_objtree_VA(createStmt, "TABLESPACE %{tablespace}I", 0);
	if (node->tablespacename)
		append_string_object(tmp, "tablespace", node->tablespacename);
	else
	{
		append_null_object(tmp, "tablespace");
		append_bool_object(tmp, "present", false);
	}
	append_object_object(createStmt, "tablespace", tmp);

	tmp = new_objtree_VA(createStmt, "ON COMMIT %{on_commit_value}s", 0);
	switch (node->oncommit)
	{
		case ONCOMMIT_DROP:
			append_string_object(tmp, "on_commit_value", "DROP");
			break;

		case ONCOMMIT_DELETE_ROWS:
			append_string_object(tmp, "on_commit_value", "DELETE ROWS");
			break;

		case ONCOMMIT_PRESERVE_ROWS:
			append_string_object(tmp, "on_commit_value", "PRESERVE ROWS");
			break;

		case ONCOMMIT_NOOP:
			append_null_object(tmp, "on_commit_value");
			append_bool_object(tmp, "present", false);
			break;
	}
	append_object_object(createStmt, "on_commit", tmp);

	command = jsonize_objtree(createStmt);

	free_objtree(createStmt);
	relation_close(relation, AccessShareLock);

	return command;
}

static char *
deparse_CompositeTypeStmt(Oid objectId, Node *parsetree)
{
	CompositeTypeStmt *node = (CompositeTypeStmt *) parsetree;
	ObjTree	   *composite;
	Relation	typerel = relation_open(objectId, AccessShareLock);
	List	   *dpcontext;
	List	   *tableelts = NIL;
	char	   *command;

	dpcontext = deparse_context_for(RelationGetRelationName(typerel),
									objectId);

	composite = new_objtree_VA(NULL,
							   "CREATE TYPE %{identity}D AS (%{columns:, }s)",
							   0);
	append_object_object(composite, "identity",
						 new_objtree_for_qualname_id(composite,
													 RelationRelationId,
													 objectId));

	tableelts = deparseTableElements(NIL, composite, typerel,
									 node->coldeflist, dpcontext,
									 false,		/* not typed */
									 true);		/* composite type */

	append_array_object(composite, "columns", tableelts);

	command = jsonize_objtree(composite);
	free_objtree(composite);
	heap_close(typerel, AccessShareLock);

	return command;
}

static char *
deparse_CreateEnumStmt(Oid objectId, Node *parsetree)
{
	CreateEnumStmt *node = (CreateEnumStmt *) parsetree;
	ObjTree	   *enumtype;
	char	   *command;
	List	   *values;
	ListCell   *cell;

	enumtype = new_objtree_VA(NULL,
							  "CREATE TYPE %{identity}D AS ENUM (%{values:, }L)",
							  0);
	append_object_object(enumtype, "identity",
						 new_objtree_for_qualname_id(enumtype,
													 TypeRelationId,
													 objectId));
	values = NIL;
	foreach(cell, node->vals)
	{
		Value   *val = (Value *) lfirst(cell);

		values = lappend(values,
						 new_string_object(enumtype, NULL, strVal(val)));
	}
	append_array_object(enumtype, "values", values);

	command = jsonize_objtree(enumtype);
	free_objtree(enumtype);

	return command;
}

static char *
deparse_CreateRangeStmt(Oid objectId, Node *parsetree)
{
	/* CreateRangeStmt *node = (CreateRangeStmt *) parsetree; */
	ObjTree	   *range;
	ObjTree	   *tmp;
	List	   *definition = NIL;
	Relation	pg_range;
	HeapTuple	rangeTup;
	Form_pg_range rangeForm;
	ScanKeyData key[1];
	SysScanDesc scan;
	char	   *command;

	pg_range = heap_open(RangeRelationId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_pg_range_rngtypid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objectId));

	scan = systable_beginscan(pg_range, RangeTypidIndexId, true,
							  NULL, 1, key);

	rangeTup = systable_getnext(scan);
	if (!HeapTupleIsValid(rangeTup))
		elog(ERROR, "cache lookup failed for range with type oid %u",
			 objectId);

	rangeForm = (Form_pg_range) GETSTRUCT(rangeTup);

	range = new_objtree_VA(NULL,
						   "CREATE RANGE %{identity}D AS RANGE (%{definition:, }s",
						   0);
	tmp = new_objtree_for_qualname_id(range, TypeRelationId, objectId);
	append_object_object(range, "identity", tmp);

	/* SUBTYPE */
	tmp = new_objtree_for_qualname_id(range,
									  TypeRelationId,
									  rangeForm->rngsubtype);
	tmp = new_objtree_VA(range,
						 "SUBTYPE = %{type}D",
						 2,
						 "clause", ObjTypeString, "subtype",
						 "type", ObjTypeObject, tmp);
	definition = lappend(definition,
						 new_object_object(range, NULL, tmp));

	/* SUBTYPE_OPCLASS */
	if (OidIsValid(rangeForm->rngsubopc))
	{
		tmp = new_objtree_for_qualname_id(range,
										  OperatorClassRelationId,
										  rangeForm->rngsubopc);
		tmp = new_objtree_VA(range,
							 "SUBTYPE_OPCLASS = %{opclass}D",
							 2,
							 "clause", ObjTypeString, "opclass",
							 "opclass", ObjTypeObject, tmp);
		definition = lappend(definition,
							 new_object_object(range, NULL, tmp));
	}

	/* COLLATION */
	if (OidIsValid(rangeForm->rngcollation))
	{
		tmp = new_objtree_for_qualname_id(range,
										  CollationRelationId,
										  rangeForm->rngcollation);
		tmp = new_objtree_VA(range,
							 "COLLATION = %{collation}D",
							 2,
							 "clause", ObjTypeString, "collation",
							 "collation", ObjTypeObject, tmp);
		definition = lappend(definition,
							 new_object_object(range, NULL, tmp));
	}

	/* CANONICAL */
	if (OidIsValid(rangeForm->rngcanonical))
	{
		tmp = new_objtree_for_qualname_id(range,
										  ProcedureRelationId,
										  rangeForm->rngcanonical);
		tmp = new_objtree_VA(range,
							 "CANONICAL = %{canonical}D",
							 2,
							 "clause", ObjTypeString, "canonical",
							 "canonical", ObjTypeObject, tmp);
		definition = lappend(definition,
							 new_object_object(range, NULL, tmp));
	}

	/* SUBTYPE_DIFF */
	if (OidIsValid(rangeForm->rngsubdiff))
	{
		tmp = new_objtree_for_qualname_id(range,
										  ProcedureRelationId,
										  rangeForm->rngsubdiff);
		tmp = new_objtree_VA(range,
							 "SUBTYPE_DIFF = %{diff}D",
							 2,
							 "clause", ObjTypeString, "subtype_diff",
							 "subtype_diff", ObjTypeObject, tmp);
		definition = lappend(definition,
							 new_object_object(range, NULL, tmp));
	}

	append_array_object(range, "definition", definition);

	systable_endscan(scan);
	heap_close(pg_range, RowExclusiveLock);

	command = jsonize_objtree(range);
	free_objtree(range);

	return command;
}

static inline ObjElem *
deparse_Seq_Cache(ObjTree *parent, Form_pg_sequence seqdata)
{
	ObjTree	   *tmp;
	char	   *tmpstr;

	tmpstr = psprintf("%lu", seqdata->cache_value);
	tmp = new_objtree_VA(parent, "CACHE %{value}s",
						 2,
						 "clause", ObjTypeString, "cache",
						 "value", ObjTypeString, tmpstr);
	return new_object_object(parent, NULL, tmp);
}

static inline ObjElem *
deparse_Seq_Cycle(ObjTree *parent, Form_pg_sequence seqdata)
{
	ObjTree	   *tmp;

	tmp = new_objtree_VA(parent, "%{no}s CYCLE",
						 2,
						 "clause", ObjTypeString, "cycle",
						 "no", ObjTypeString,
						 seqdata->is_cycled ? "" : "NO");
	return new_object_object(parent, NULL, tmp);
}

static inline ObjElem *
deparse_Seq_IncrementBy(ObjTree *parent, Form_pg_sequence seqdata)
{
	ObjTree	   *tmp;
	char	   *tmpstr;

	tmpstr = psprintf("%lu", seqdata->increment_by);
	tmp = new_objtree_VA(parent, "INCREMENT BY %{value}s",
						 2,
						 "clause", ObjTypeString, "increment_by",
						 "value", ObjTypeString, tmpstr);
	return new_object_object(parent, NULL, tmp);
}

static inline ObjElem *
deparse_Seq_Minvalue(ObjTree *parent, Form_pg_sequence seqdata)
{
	ObjTree	   *tmp;
	char	   *tmpstr;

	tmpstr = psprintf("%lu", seqdata->min_value);
	tmp = new_objtree_VA(parent, "MINVALUE %{value}s",
						 2,
						 "clause", ObjTypeString, "minvalue",
						 "value", ObjTypeString, tmpstr);
	return new_object_object(parent, NULL, tmp);
}

static inline ObjElem *
deparse_Seq_Maxvalue(ObjTree *parent, Form_pg_sequence seqdata)
{
	ObjTree	   *tmp;
	char	   *tmpstr;

	tmpstr = psprintf("%lu", seqdata->max_value);
	tmp = new_objtree_VA(parent, "MAXVALUE %{value}s",
						 2,
						 "clause", ObjTypeString, "maxvalue",
						 "value", ObjTypeString, tmpstr);
	return new_object_object(parent, NULL, tmp);
}

static inline ObjElem *
deparse_Seq_Startwith(ObjTree *parent, Form_pg_sequence seqdata)
{
	ObjTree	   *tmp;
	char	   *tmpstr;

	tmpstr = psprintf("%lu", seqdata->start_value);
	tmp = new_objtree_VA(parent, "START WITH %{value}s",
						 2,
						 "clause", ObjTypeString, "start",
						 "value", ObjTypeString, tmpstr);
	return new_object_object(parent, NULL, tmp);
}

static inline ObjElem *
deparse_Seq_Restart(ObjTree *parent, Form_pg_sequence seqdata)
{
	ObjTree	   *tmp;
	char	   *tmpstr;

	tmpstr = psprintf("%lu", seqdata->last_value);
	tmp = new_objtree_VA(parent, "RESTART %{value}s",
						 2,
						 "clause", ObjTypeString, "restart",
						 "value", ObjTypeString, tmpstr);
	return new_object_object(parent, NULL, tmp);
}

static ObjElem *
deparse_Seq_OwnedBy(ObjTree *parent, Oid sequenceId)
{
	ObjTree    *ownedby = NULL;
	Relation	depRel;
	SysScanDesc scan;
	ScanKeyData keys[3];
	HeapTuple	tuple;

	depRel = heap_open(DependRelationId, AccessShareLock);
	ScanKeyInit(&keys[0],
				Anum_pg_depend_classid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationRelationId));
	ScanKeyInit(&keys[1],
				Anum_pg_depend_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(sequenceId));
	ScanKeyInit(&keys[2],
				Anum_pg_depend_objsubid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(0));

	scan = systable_beginscan(depRel, DependDependerIndexId, true,
							  NULL, 3, keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Oid			ownerId;
		Form_pg_depend depform;
		ObjTree    *tmp;
		char	   *colname;

		depform = (Form_pg_depend) GETSTRUCT(tuple);

		/* only consider AUTO dependencies on pg_class */
		if (depform->deptype != DEPENDENCY_AUTO)
			continue;
		if (depform->refclassid != RelationRelationId)
			continue;
		if (depform->refobjsubid <= 0)
			continue;

		ownerId = depform->refobjid;
		colname = get_attname(ownerId, depform->refobjsubid);
		if (colname == NULL)
			continue;

		tmp = new_objtree_for_qualname_id(parent, RelationRelationId, ownerId);
		append_string_object(tmp, "attrname", colname);
		ownedby = new_objtree_VA(parent,
								 "OWNED BY %{owner}D",
								 2,
								 "clause", ObjTypeString, "owned",
								 "owner", ObjTypeObject, tmp);
	}

	systable_endscan(scan);
	relation_close(depRel, AccessShareLock);

	/*
	 * If there's no owner column, emit an empty OWNED BY element, set up so
	 * that it won't print anything.
	 */
	if (!ownedby)
		/* XXX this shouldn't happen ... */
		ownedby = new_objtree_VA(parent,
								 "OWNED BY %{owner}D",
								 3,
								 "clause", ObjTypeString, "owned",
								 "owner", ObjTypeNull,
								 "present", ObjTypeBool, false);
	return new_object_object(parent, NULL, ownedby);
}

/*
 * deparse_CreateSeqStmt
 *		deparse a CreateSeqStmt
 *
 * Given a sequence OID and the parsetree that created it, return the JSON blob
 * representing the creation command.
 */
static char *
deparse_CreateSeqStmt(Oid objectId, Node *parsetree)
{
	ObjTree    *createSeq;
	ObjTree    *tmp;
	Relation	relation = relation_open(objectId, AccessShareLock);
	char	   *command;
	Form_pg_sequence seqdata;
	List	   *elems = NIL;

	seqdata = get_sequence_values(objectId);

	createSeq =
		new_objtree_VA(NULL,
					   "CREATE %{persistence}s SEQUENCE %{identity}D "
					   "%{definition: }s",
					   1,
					   "persistence", ObjTypeString,
					   get_persistence_str(relation->rd_rel->relpersistence));

	tmp = new_objtree_for_qualname(createSeq,
								   relation->rd_rel->relnamespace,
								   RelationGetRelationName(relation));
	append_object_object(createSeq, "identity", tmp);

	/* definition elements */
	elems = lappend(elems, deparse_Seq_Cache(createSeq, seqdata));
	elems = lappend(elems, deparse_Seq_Cycle(createSeq, seqdata));
	elems = lappend(elems, deparse_Seq_IncrementBy(createSeq, seqdata));
	elems = lappend(elems, deparse_Seq_Minvalue(createSeq, seqdata));
	elems = lappend(elems, deparse_Seq_Maxvalue(createSeq, seqdata));
	elems = lappend(elems, deparse_Seq_Startwith(createSeq, seqdata));
	elems = lappend(elems, deparse_Seq_Restart(createSeq, seqdata));
	/* we purposefully do not emit OWNED BY here */

	append_array_object(createSeq, "definition", elems);

	command = jsonize_objtree(createSeq);

	free_objtree(createSeq);
	relation_close(relation, AccessShareLock);

	return command;
}

static char *
deparse_AlterSeqStmt(Oid objectId, Node *parsetree)
{
	ObjTree	   *alterSeq;
	ObjTree	   *tmp;
	Relation	relation = relation_open(objectId, AccessShareLock);
	char	   *command;
	Form_pg_sequence seqdata;
	List	   *elems = NIL;
	ListCell   *cell;

	seqdata = get_sequence_values(objectId);

	alterSeq =
		new_objtree_VA(NULL,
					   "ALTER SEQUENCE %{identity}D %{definition: }s", 0);
	tmp = new_objtree_for_qualname(alterSeq,
								   relation->rd_rel->relnamespace,
								   RelationGetRelationName(relation));
	append_object_object(alterSeq, "identity", tmp);

	foreach(cell, ((AlterSeqStmt *) parsetree)->options)
	{
		DefElem *elem = (DefElem *) lfirst(cell);
		ObjElem *newelm;

		if (strcmp(elem->defname, "cache") == 0)
			newelm = deparse_Seq_Cache(alterSeq, seqdata);
		else if (strcmp(elem->defname, "cycle") == 0)
			newelm = deparse_Seq_Cycle(alterSeq, seqdata);
		else if (strcmp(elem->defname, "increment_by") == 0)
			newelm = deparse_Seq_IncrementBy(alterSeq, seqdata);
		else if (strcmp(elem->defname, "minvalue") == 0)
			newelm = deparse_Seq_Minvalue(alterSeq, seqdata);
		else if (strcmp(elem->defname, "maxvalue") == 0)
			newelm = deparse_Seq_Maxvalue(alterSeq, seqdata);
		else if (strcmp(elem->defname, "start") == 0)
			newelm = deparse_Seq_Startwith(alterSeq, seqdata);
		else if (strcmp(elem->defname, "restart") == 0)
			newelm = deparse_Seq_Restart(alterSeq, seqdata);
		else if (strcmp(elem->defname, "owned_by") == 0)
			newelm = deparse_Seq_OwnedBy(alterSeq, objectId);
		else
			elog(ERROR, "invalid sequence option %s", elem->defname);

		elems = lappend(elems, newelm);
	}

	append_array_object(alterSeq, "definition", elems);

	command = jsonize_objtree(alterSeq);

	free_objtree(alterSeq);
	relation_close(relation, AccessShareLock);

	return command;
}

/*
 * deparse_IndexStmt
 *		deparse an IndexStmt
 *
 * Given an index OID and the parsetree that created it, return the JSON blob
 * representing the creation command.
 *
 * If the index corresponds to a constraint, NULL is returned.
 */
static char *
deparse_IndexStmt(Oid objectId, Node *parsetree)
{
	IndexStmt  *node = (IndexStmt *) parsetree;
	ObjTree    *indexStmt;
	ObjTree    *tmp;
	Relation	idxrel;
	Relation	heaprel;
	char	   *command;
	char	   *index_am;
	char	   *definition;
	char	   *reloptions;
	char	   *tablespace;
	char	   *whereClause;

	if (node->primary || node->isconstraint)
	{
		/*
		 * indexes for PRIMARY KEY and other constraints are output
		 * separately; return empty here.
		 */
		return NULL;
	}

	idxrel = relation_open(objectId, AccessShareLock);
	heaprel = relation_open(idxrel->rd_index->indrelid, AccessShareLock);

	pg_get_indexdef_detailed(objectId,
							 &index_am, &definition, &reloptions,
							 &tablespace, &whereClause);

	indexStmt =
		new_objtree_VA(NULL,
					   "CREATE %{unique}s INDEX %{concurrently}s %{name}I "
					   "ON %{table}D USING %{index_am}s (%{definition}s) "
					   "%{with}s %{tablespace}s %{where_clause}s",
					   5,
					   "unique", ObjTypeString, node->unique ? "UNIQUE" : "",
					   "concurrently", ObjTypeString,
					   node->concurrent ? "CONCURRENTLY" : "",
					   "name", ObjTypeString, RelationGetRelationName(idxrel),
					   "definition", ObjTypeString, definition,
					   "index_am", ObjTypeString, index_am);

	tmp = new_objtree_for_qualname(indexStmt,
								   heaprel->rd_rel->relnamespace,
								   RelationGetRelationName(heaprel));
	append_object_object(indexStmt, "table", tmp);

	/* reloptions */
	tmp = new_objtree_VA(indexStmt, "WITH (%{opts}s)", 0);
	if (reloptions)
		append_string_object(tmp, "opts", reloptions);
	else
		append_bool_object(tmp, "present", false);
	append_object_object(indexStmt, "with", tmp);

	/* tablespace */
	tmp = new_objtree_VA(indexStmt, "TABLESPACE %{tablespace}s", 0);
	if (tablespace)
		append_string_object(tmp, "tablespace", tablespace);
	else
		append_bool_object(tmp, "present", false);
	append_object_object(indexStmt, "tablespace", tmp);

	/* WHERE clause */
	tmp = new_objtree_VA(indexStmt, "WHERE %{where}s", 0);
	if (whereClause)
		append_string_object(tmp, "where", whereClause);
	else
		append_bool_object(tmp, "present", false);
	append_object_object(indexStmt, "where_clause", tmp);

	command = jsonize_objtree(indexStmt);
	free_objtree(indexStmt);

	heap_close(idxrel, AccessShareLock);
	heap_close(heaprel, AccessShareLock);

	return command;
}

/*
 * deparse_CreateSchemaStmt
 *		deparse a CreateSchemaStmt
 *
 * Given a schema OID and the parsetree that created it, return the JSON blob
 * representing the creation command.
 *
 * Note we don't output the schema elements given in the creation command.
 * They must be output separately.	 (In the current implementation,
 * CreateSchemaCommand passes them back to ProcessUtility, which will lead to
 * this file if appropriate.)
 */
static char *
deparse_CreateSchemaStmt(Oid objectId, Node *parsetree)
{
	CreateSchemaStmt *node = (CreateSchemaStmt *) parsetree;
	ObjTree    *createSchema;
	ObjTree    *auth;
	char	   *command;

	createSchema =
		new_objtree_VA(NULL,
				"CREATE SCHEMA %{if_not_exists}s %{name}I %{authorization}s",
					   2,
					   "name", ObjTypeString, node->schemaname,
					   "if_not_exists", ObjTypeString,
					   node->if_not_exists ? "IF NOT EXISTS" : "");

	auth = new_objtree_VA(createSchema,
						  "AUTHORIZATION %{authorization_role}I", 0);
	if (node->authid)
		append_string_object(auth, "authorization_role", node->authid);
	else
	{
		append_null_object(auth, "authorization_role");
		append_bool_object(auth, "present", false);
	}
	append_object_object(createSchema, "authorization", auth);

	command = jsonize_objtree(createSchema);
	free_objtree(createSchema);

	return command;
}

/*
 * Given a utility command parsetree and the OID of the corresponding object,
 * return a JSON representation of the command.
 *
 * The command is expanded fully, so that there are no ambiguities even in the
 * face of search_path changes.
 */
char *
deparse_utility_command(Oid objectId, Node *parsetree)
{
	OverrideSearchPath *overridePath;
	char	   *command;

	/*
	 * Many routines underlying this one will invoke ruleutils.c functionality
	 * in order to obtain deparsed versions of expressions.  In such results,
	 * we want all object names to be qualified, so that results are "portable"
	 * to environments with different search_path settings.  Rather than inject
	 * what would be repetitive calls to override search path all over the
	 * place, we do it centrally here.
	 */
	overridePath = GetOverrideSearchPath(CurrentMemoryContext);
	overridePath->schemas = NIL;
	PushOverrideSearchPath(overridePath);

	switch (nodeTag(parsetree))
	{
		case T_CreateSchemaStmt:
			command = deparse_CreateSchemaStmt(objectId, parsetree);
			break;

		case T_CreateStmt:
			command = deparse_CreateStmt(objectId, parsetree);
			break;

		case T_IndexStmt:
			command = deparse_IndexStmt(objectId, parsetree);
			break;

		case T_ViewStmt:
			command = deparse_ViewStmt(objectId, parsetree);
			break;

		case T_CreateSeqStmt:
			command = deparse_CreateSeqStmt(objectId, parsetree);
			break;

		case T_AlterSeqStmt:
			command = deparse_AlterSeqStmt(objectId, parsetree);
			break;

			/* creation of objects hanging off tables */
		case T_CreateTrigStmt:
			command = deparse_CreateTrigStmt(objectId, parsetree);
			break;

		case T_RuleStmt:
			command = NULL;
			break;

			/* FDW-related objects */
		case T_CreateForeignTableStmt:
		case T_CreateFdwStmt:
		case T_CreateForeignServerStmt:
		case T_CreateUserMappingStmt:

			/* other local objects */
		case T_DefineStmt:
		case T_CreateExtensionStmt:
			command = NULL;
			break;

		case T_CompositeTypeStmt:		/* CREATE TYPE (composite) */
			command = deparse_CompositeTypeStmt(objectId, parsetree);
			break;

		case T_CreateEnumStmt:	/* CREATE TYPE AS ENUM */
			command = deparse_CreateEnumStmt(objectId, parsetree);
			break;

		case T_CreateRangeStmt:	/* CREATE TYPE AS RANGE */
			command = deparse_CreateRangeStmt(objectId, parsetree);
			break;

		case T_CreateDomainStmt:
		case T_CreateFunctionStmt:
		case T_CreateTableAsStmt:
		case T_CreatePLangStmt:
		case T_CreateConversionStmt:
		case T_CreateCastStmt:
		case T_CreateOpClassStmt:
		case T_CreateOpFamilyStmt:
			command = NULL;
			break;

			/* matviews */
		case T_RefreshMatViewStmt:
			command = NULL;
			break;

		default:
			command = NULL;
			elog(LOG, "unrecognized node type: %d",
				 (int) nodeTag(parsetree));
	}

	PopOverrideSearchPath();

	return command;
}
