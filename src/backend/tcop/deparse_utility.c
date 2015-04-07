/*-------------------------------------------------------------------------
 *
 * deparse_utility.c
 *	  Functions to convert utility commands to machine-parseable
 *	  representation
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
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
#include "access/sysattr.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_policy.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_range.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_config_map.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "parser/analyze.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "rewrite/rewriteHandler.h"
#include "tcop/deparse_utility.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"


/*
 * Before they are turned into JSONB representation, each command is
 * represented as an object tree, using the structs below.
 */
typedef enum
{
	ObjTypeNull,
	ObjTypeBool,
	ObjTypeString,
	ObjTypeArray,
	ObjTypeInteger,
	ObjTypeFloat,
	ObjTypeObject
} ObjType;

typedef struct ObjTree
{
	slist_head	params;
	int			numParams;
} ObjTree;

typedef struct ObjElem
{
	char	   *name;
	ObjType		objtype;

	union
	{
		bool		boolean;
		char	   *string;
		int64		integer;
		float8		flt;
		ObjTree	   *object;
		List	   *array;
	} value;
	slist_node	node;
} ObjElem;

static ObjElem *new_null_object(void);
static ObjElem *new_bool_object(bool value);
static ObjElem *new_string_object(char *value);
static ObjElem *new_object_object(ObjTree *value);
static ObjElem *new_array_object(List *array);
static ObjElem *new_integer_object(int64 value);
static ObjElem *new_float_object(float8 value);
static void append_null_object(ObjTree *tree, char *name);
static void append_bool_object(ObjTree *tree, char *name, bool value);
static void append_string_object(ObjTree *tree, char *name, char *value);
static void append_object_object(ObjTree *tree, char *name, ObjTree *value);
static void append_array_object(ObjTree *tree, char *name, List *array);
#ifdef NOT_USED
static void append_integer_object(ObjTree *tree, char *name, int64 value);
#endif
static void append_float_object(ObjTree *tree, char *name, float8 value);
static inline void append_premade_object(ObjTree *tree, ObjElem *elem);
static JsonbValue *objtree_to_jsonb_rec(ObjTree *tree, JsonbParseState *state);
static const char *stringify_objtype(ObjectType objtype);

/*
 * Allocate a new object tree to store parameter values.
 */
static ObjTree *
new_objtree(void)
{
	ObjTree    *params;

	params = palloc(sizeof(ObjTree));
	params->numParams = 0;
	slist_init(&params->params);

	return params;
}

/*
 * Allocate a new object tree to store parameter values -- varargs version.
 *
 * The "fmt" argument is used to append as a "fmt" element in the output blob.
 * numobjs indicates the number of extra elements to append; for each one, a
 * name (string), type (from the ObjType enum) and value must be supplied.  The
 * value must match the type given; for instance, ObjTypeInteger requires an
 * int64, ObjTypeString requires a char *, ObjTypeArray requires a list (of
 * ObjElem), ObjTypeObject requires an ObjTree, and so on.  Each element type *
 * must match the conversion specifier given in the format string, as described
 * in pg_event_trigger_expand_command, q.v.
 *
 * Note we don't have the luxury of sprintf-like compiler warnings for
 * malformed argument lists.
 */
static ObjTree *
new_objtree_VA(char *fmt, int numobjs,...)
{
	ObjTree    *tree;
	va_list		args;
	int			i;

	/* Set up the toplevel object and its "fmt" */
	tree = new_objtree();
	append_string_object(tree, "fmt", fmt);

	/* And process the given varargs */
	va_start(args, numobjs);
	for (i = 0; i < numobjs; i++)
	{
		char	   *name;
		ObjType		type;
		ObjElem	   *elem;

		name = va_arg(args, char *);
		type = va_arg(args, ObjType);

		/*
		 * For all other param types there must be a value in the varargs.
		 * Fetch it and add the fully formed subobject into the main object.
		 */
		switch (type)
		{
			case ObjTypeBool:
				elem = new_bool_object(va_arg(args, int));
				break;
			case ObjTypeString:
				elem = new_string_object(va_arg(args, char *));
				break;
			case ObjTypeObject:
				elem = new_object_object(va_arg(args, ObjTree *));
				break;
			case ObjTypeArray:
				elem = new_array_object(va_arg(args, List *));
				break;
			case ObjTypeInteger:
				elem = new_integer_object(va_arg(args, int64));
				break;
			case ObjTypeFloat:
				elem = new_float_object(va_arg(args, double));
				break;
			case ObjTypeNull:
				/* Null params don't have a value (obviously) */
				elem = new_null_object();
				break;
			default:
				elog(ERROR, "invalid ObjTree element type %d", type);
		}

		elem->name = name;
		append_premade_object(tree, elem);
	}

	va_end(args);
	return tree;
}

/* Allocate a new parameter with a NULL value */
static ObjElem *
new_null_object(void)
{
	ObjElem    *param;

	param = palloc0(sizeof(ObjElem));

	param->name = NULL;
	param->objtype = ObjTypeNull;

	return param;
}

/* Append a NULL object to a tree */
static void
append_null_object(ObjTree *tree, char *name)
{
	ObjElem    *param;

	param = new_null_object();
	param->name = name;
	append_premade_object(tree, param);
}

/* Allocate a new boolean parameter */
static ObjElem *
new_bool_object(bool value)
{
	ObjElem    *param;

	param = palloc0(sizeof(ObjElem));
	param->name = NULL;
	param->objtype = ObjTypeBool;
	param->value.boolean = value;

	return param;
}

/* Append a boolean parameter to a tree */
static void
append_bool_object(ObjTree *tree, char *name, bool value)
{
	ObjElem    *param;

	param = new_bool_object(value);
	param->name = name;
	append_premade_object(tree, param);
}

/* Allocate a new string object */
static ObjElem *
new_string_object(char *value)
{
	ObjElem    *param;

	Assert(value);

	param = palloc0(sizeof(ObjElem));
	param->name = NULL;
	param->objtype = ObjTypeString;
	param->value.string = value;

	return param;
}

/*
 * Append a string parameter to a tree.
 */
static void
append_string_object(ObjTree *tree, char *name, char *value)
{
	ObjElem	   *param;

	Assert(name);
	param = new_string_object(value);
	param->name = name;
	append_premade_object(tree, param);
}

static ObjElem *
new_integer_object(int64 value)
{
	ObjElem	   *param;

	param = palloc0(sizeof(ObjElem));
	param->name = NULL;
	param->objtype = ObjTypeInteger;
	param->value.integer = value;

	return param;
}

static ObjElem *
new_float_object(float8 value)
{
	ObjElem	   *param;

	param = palloc0(sizeof(ObjElem));
	param->name = NULL;
	param->objtype = ObjTypeFloat;
	param->value.flt = value;

	return param;
}

#ifdef NOT_USED
/*
 * Append an int64 parameter to a tree.
 */
static void
append_integer_object(ObjTree *tree, char *name, int64 value)
{
	ObjElem	   *param;

	param = new_integer_object(value);
	param->name = name;
	append_premade_object(tree, param);
}
#endif

/*
 * Append a float8 parameter to a tree.
 */
static void
append_float_object(ObjTree *tree, char *name, float8 value)
{
	ObjElem	   *param;

	param = new_float_object(value);
	param->name = name;
	append_premade_object(tree, param);
}

/* Allocate a new object parameter */
static ObjElem *
new_object_object(ObjTree *value)
{
	ObjElem    *param;

	param = palloc0(sizeof(ObjElem));
	param->name = NULL;
	param->objtype = ObjTypeObject;
	param->value.object = value;

	return param;
}

/* Append an object parameter to a tree */
static void
append_object_object(ObjTree *tree, char *name, ObjTree *value)
{
	ObjElem    *param;

	Assert(name);
	param = new_object_object(value);
	param->name = name;
	append_premade_object(tree, param);
}

/* Allocate a new array parameter */
static ObjElem *
new_array_object(List *array)
{
	ObjElem    *param;

	param = palloc0(sizeof(ObjElem));
	param->name = NULL;
	param->objtype = ObjTypeArray;
	param->value.array = array;

	return param;
}

/* Append an array parameter to a tree */
static void
append_array_object(ObjTree *tree, char *name, List *array)
{
	ObjElem    *param;

	param = new_array_object(array);
	param->name = name;
	append_premade_object(tree, param);
}

/* Append a preallocated parameter to a tree */
static inline void
append_premade_object(ObjTree *tree, ObjElem *elem)
{
	slist_push_head(&tree->params, &elem->node);
	tree->numParams++;
}

/*
 * Helper for objtree_to_jsonb: process an individual element from an object or
 * an array into the output parse state.
 */
static void
objtree_to_jsonb_element(JsonbParseState *state, ObjElem *object,
						 JsonbIteratorToken elem_token)
{
	ListCell   *cell;
	JsonbValue	val;

	switch (object->objtype)
	{
		case ObjTypeNull:
			val.type = jbvNull;
			pushJsonbValue(&state, elem_token, &val);
			break;

		case ObjTypeString:
			val.type = jbvString;
			val.val.string.len = strlen(object->value.string);
			val.val.string.val = object->value.string;
			pushJsonbValue(&state, elem_token, &val);
			break;

		case ObjTypeInteger:
			val.type = jbvNumeric;
			val.val.numeric = (Numeric)
				DatumGetNumeric(DirectFunctionCall1(int8_numeric,
													object->value.integer));
			pushJsonbValue(&state, elem_token, &val);
			break;

		case ObjTypeFloat:
			val.type = jbvNumeric;
			val.val.numeric = (Numeric)
				DatumGetNumeric(DirectFunctionCall1(float8_numeric,
													object->value.integer));
			pushJsonbValue(&state, elem_token, &val);
			break;

		case ObjTypeBool:
			val.type = jbvBool;
			val.val.boolean = object->value.boolean;
			pushJsonbValue(&state, elem_token, &val);
			break;

		case ObjTypeObject:
			/* recursively add the object into the existing parse state */
			objtree_to_jsonb_rec(object->value.object, state);
			break;

		case ObjTypeArray:
			pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);
			foreach(cell, object->value.array)
			{
				ObjElem   *elem = lfirst(cell);

				objtree_to_jsonb_element(state, elem, WJB_ELEM);
			}
			pushJsonbValue(&state, WJB_END_ARRAY, NULL);
			break;

		default:
			elog(ERROR, "unrecognized object type %d", object->objtype);
			break;
	}
}

/*
 * Recursive helper for objtree_to_jsonb
 */
static JsonbValue *
objtree_to_jsonb_rec(ObjTree *tree, JsonbParseState *state)
{
	slist_iter	iter;

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	slist_foreach(iter, &tree->params)
	{
		ObjElem    *object = slist_container(ObjElem, node, iter.cur);
		JsonbValue	key;

		/* Push the key first */
		key.type = jbvString;
		key.val.string.len = strlen(object->name);
		key.val.string.val = object->name;
		pushJsonbValue(&state, WJB_KEY, &key);

		/* Then process the value according to its type */
		objtree_to_jsonb_element(state, object, WJB_VALUE);
	}

	return pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

/*
 * Create a JSONB representation from an ObjTree.
 */
static Jsonb *
objtree_to_jsonb(ObjTree *tree)
{
	JsonbValue *value;

	value = objtree_to_jsonb_rec(tree, NULL);
	return JsonbValueToJsonb(value);
}

/*
 * A helper routine to setup %{}T elements.
 */
static ObjTree *
new_objtree_for_type(Oid typeId, int32 typmod)
{
	ObjTree    *typeParam;
	Oid			typnspid;
	char	   *typnsp;
	char	   *typename;
	char	   *typmodstr;
	bool		typarray;

	format_type_detailed(typeId, typmod,
						 &typnspid, &typename, &typmodstr, &typarray);

	if (!OidIsValid(typnspid))
		typnsp = pstrdup("");
	else if (isAnyTempNamespace(typnspid))
		typnsp = pstrdup("pg_temp");
	else
		typnsp = get_namespace_name(typnspid);

	/* We don't use new_objtree_VA here because types don't have a "fmt" */
	typeParam = new_objtree();
	append_string_object(typeParam, "schemaname", typnsp);
	append_string_object(typeParam, "typename", typename);
	append_string_object(typeParam, "typmod", typmodstr);
	append_bool_object(typeParam, "typarray", typarray);

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
new_objtree_for_qualname(Oid nspid, char *name)
{
	ObjTree    *qualified;
	char	   *namespace;

	/*
	 * We don't use new_objtree_VA here because these names don't have a "fmt"
	 */
	qualified = new_objtree();
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
new_objtree_for_qualname_id(Oid classId, Oid objectId)
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

	qualified = new_objtree_for_qualname(DatumGetObjectId(objnsp),
										 NameStr(*DatumGetName(objname)));
	heap_close(catalog, AccessShareLock);

	return qualified;
}

/*
 * Helper routine for %{}R objects, with role specified by OID.  (ACL_ID_PUBLIC
 * means to use "public").
 */
static ObjTree *
new_objtree_for_role_id(Oid roleoid)
{
	ObjTree    *role;

	role = new_objtree();
	append_bool_object(role, "is_public", roleoid == ACL_ID_PUBLIC);

	if (roleoid != ACL_ID_PUBLIC)
	{
		HeapTuple	roltup;
		char	   *rolename;

		roltup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleoid));
		if (!HeapTupleIsValid(roltup))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("role with OID %u does not exist", roleoid)));

		rolename = NameStr(((Form_pg_authid) GETSTRUCT(roltup))->rolname);
		append_string_object(role, "rolename", pstrdup(rolename));
		ReleaseSysCache(roltup);
	}

	return role;
}

/*
 * Helper routine for %{}R objects, with role specified by name.
 */
static ObjTree *
new_objtree_for_role(char *rolename)
{
	ObjTree	   *role;
	bool		is_public;

	role = new_objtree();
	is_public = strcmp(rolename, "public") == 0;
	append_bool_object(role, "is_public", is_public);
	if (!is_public)
		append_string_object(role, "rolename", rolename);

	return role;
}

/*
 * Helper routine for %{}R objects, with role specified by RoleSpec node.
 * Special values such as ROLESPEC_CURRENT_USER are expanded to their final
 * names.
 */
static ObjTree *
new_objtree_for_rolespec(RoleSpec *spec)
{
	ObjTree	   *role;

	role = new_objtree();
	append_bool_object(role, "is_public",
					   spec->roletype == ROLESPEC_PUBLIC);
	if (spec->roletype != ROLESPEC_PUBLIC)
		append_string_object(role, "rolename", get_rolespec_name((Node *) spec));

	return role;
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
 * deparse_CreateExtensionStmt
 *		deparse a CreateExtensionStmt
 *
 * Given an extension OID and the parsetree that created it, return the JSON
 * blob representing the creation command.
 *
 * XXX the current representation makes the output command dependant on the
 * installed versions of the extension.  Is this a problem?
 */
static ObjTree *
deparse_CreateExtensionStmt(Oid objectId, Node *parsetree)
{
	CreateExtensionStmt *node = (CreateExtensionStmt *) parsetree;
	Relation    pg_extension;
	HeapTuple   extTup;
	Form_pg_extension extForm;
	ObjTree	   *extStmt;
	ObjTree	   *tmp;
	List	   *list;
	ListCell   *cell;

	pg_extension = heap_open(ExtensionRelationId, AccessShareLock);
	extTup = get_catalog_object_by_oid(pg_extension, objectId);
	if (!HeapTupleIsValid(extTup))
		elog(ERROR, "cache lookup failed for extension with OID %u",
			 objectId);
	extForm = (Form_pg_extension) GETSTRUCT(extTup);

	extStmt = new_objtree_VA("CREATE EXTENSION %{if_not_exists}s %{identity}I "
							 "%{options: }s",
							 1, "identity", ObjTypeString, node->extname);
	append_string_object(extStmt, "if_not_exists",
						 node->if_not_exists ? "IF NOT EXISTS" : "");
	list = NIL;
	foreach(cell, node->options)
	{
		DefElem *opt = (DefElem *) lfirst(cell);

		if (strcmp(opt->defname, "schema") == 0)
		{
			/* skip this one; we add one unconditionally below */
			continue;
		}
		else if (strcmp(opt->defname, "new_version") == 0)
		{
			tmp = new_objtree_VA("VERSION %{version}L", 2,
								 "type", ObjTypeString, "version",
								 "version", ObjTypeString, defGetString(opt));
			list = lappend(list, new_object_object(tmp));
		}
		else if (strcmp(opt->defname, "old_version") == 0)
		{
			tmp = new_objtree_VA("FROM %{version}L", 2,
								 "type", ObjTypeString, "from",
								 "version", ObjTypeString, defGetString(opt));
			list = lappend(list, new_object_object(tmp));
		}
		else
			elog(ERROR, "unsupported option %s", opt->defname);
	}

	tmp = new_objtree_VA("SCHEMA %{schema}I",
						 2, "type", ObjTypeString, "schema",
						 "schema", ObjTypeString,
						 get_namespace_name(extForm->extnamespace));
	list = lappend(list, new_object_object(tmp));

	append_array_object(extStmt, "options", list);

	heap_close(pg_extension, AccessShareLock);

	return extStmt;
}

static ObjTree *
deparse_AlterExtensionStmt(Oid objectId, Node *parsetree)
{
	AlterExtensionStmt *node = (AlterExtensionStmt *) parsetree;
	Relation    pg_extension;
	HeapTuple   extTup;
	Form_pg_extension extForm;
	ObjTree	   *stmt;
	ObjTree	   *tmp;
	List	   *list = NIL;
	ListCell   *cell;

	pg_extension = heap_open(ExtensionRelationId, AccessShareLock);
	extTup = get_catalog_object_by_oid(pg_extension, objectId);
	if (!HeapTupleIsValid(extTup))
		elog(ERROR, "cache lookup failed for extension with OID %u",
			 objectId);
	extForm = (Form_pg_extension) GETSTRUCT(extTup);

	stmt = new_objtree_VA("ALTER EXTENSION %{identity}I UPDATE %{options: }s", 1,
						  "identity", ObjTypeString,
						  NameStr(extForm->extname));

	foreach(cell, node->options)
	{
		DefElem *opt = (DefElem *) lfirst(cell);

		if (strcmp(opt->defname, "new_version") == 0)
		{
			tmp = new_objtree_VA("TO %{version}L", 2,
								 "type", ObjTypeString, "version",
								 "version", ObjTypeString, defGetString(opt));
			list = lappend(list, new_object_object(tmp));
		}
		else
			elog(ERROR, "unsupported option %s", opt->defname);
	}

	append_array_object(stmt, "options", list);

	heap_close(pg_extension, AccessShareLock);

	return stmt;
}

static ObjTree *
deparse_AlterExtensionContentsStmt(Oid objectId, Node *parsetree,
								   ObjectAddress objectAddress)
{
	AlterExtensionContentsStmt *node = (AlterExtensionContentsStmt *) parsetree;
	ObjTree	   *stmt;
	char	   *fmt;

	Assert(node->action == +1 || node->action == -1);

	fmt = psprintf("ALTER EXTENSION %%{extidentity}I %s %s %%{objidentity}s",
				   node->action == +1 ? "ADD" : "DROP",
				   stringify_objtype(node->objtype));

	stmt = new_objtree_VA(fmt, 2, "extidentity", ObjTypeString, node->extname,
						  "objidentity", ObjTypeString,
						  getObjectIdentity(&objectAddress));

	return stmt;
}

static ObjTree *
deparse_AlterDomainStmt(Oid objectId, Node *parsetree,
						ObjectAddress constraintAddr)
{
	AlterDomainStmt *node = (AlterDomainStmt *) parsetree;
	HeapTuple	domTup;
	Form_pg_type domForm;
	ObjTree	   *alterDom;
	char	   *fmt;

	/* ALTER DOMAIN DROP CONSTRAINT is handled by the DROP support code */
	if (node->subtype == 'X')
		return NULL;

	domTup = SearchSysCache1(TYPEOID, objectId);
	if (!HeapTupleIsValid(domTup))
		elog(ERROR, "cache lookup failed for domain with OID %u",
			 objectId);
	domForm = (Form_pg_type) GETSTRUCT(domTup);

	switch (node->subtype)
	{
		case 'T':
			/* SET DEFAULT / DROP DEFAULT */
			if (node->def == NULL)
				fmt = "ALTER DOMAIN %{identity}D DROP DEFAULT";
			else
				fmt = "ALTER DOMAIN %{identity}D SET DEFAULT %{default}s";
			break;
		case 'N':
			/* DROP NOT NULL */
			fmt = "ALTER DOMAIN %{identity}D DROP NOT NULL";
			break;
		case 'O':
			/* SET NOT NULL */
			fmt = "ALTER DOMAIN %{identity}D SET NOT NULL";
			break;
		case 'C':
			/* ADD CONSTRAINT.  Only CHECK constraints are supported by domains */
			fmt = "ALTER DOMAIN %{identity}D ADD CONSTRAINT %{constraint_name}I %{definition}s";
			break;
		case 'V':
			/* VALIDATE CONSTRAINT */
			fmt = "ALTER DOMAIN %{identity}D VALIDATE CONSTRAINT %{constraint_name}I";
			break;
		default:
			elog(ERROR, "invalid subtype %c", node->subtype);
	}

	alterDom = new_objtree_VA(fmt, 0);
	append_object_object(alterDom, "identity",
						 new_objtree_for_qualname(domForm->typnamespace,
												  NameStr(domForm->typname)));

	/*
	 * Process subtype-specific options.  Validating a constraint
	 * requires its name ...
	 */
	if (node->subtype == 'V')
		append_string_object(alterDom, "constraint_name", node->name);

	/* ... a new constraint has a name and definition ... */
	if (node->subtype == 'C')
	{
		append_string_object(alterDom, "definition",
							 pg_get_constraintdef_string(constraintAddr.objectId,
														 false));
		/* can't rely on node->name here; might not be defined */
		append_string_object(alterDom, "constraint_name",
							 get_constraint_name(constraintAddr.objectId));
	}

	/* ... and setting a default has a definition only. */
	if (node->subtype == 'T' && node->def != NULL)
		append_string_object(alterDom, "default", DomainGetDefault(domTup));

	/* done */
	ReleaseSysCache(domTup);

	return alterDom;
}

/*
 * deparse_CreateTrigStmt
 *		Deparse a CreateTrigStmt (CREATE TRIGGER)
 *
 * Given a trigger OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 */
static ObjTree *
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

	pg_trigger = heap_open(TriggerRelationId, AccessShareLock);

	trigTup = get_catalog_object_by_oid(pg_trigger, objectId);
	trigForm = (Form_pg_trigger) GETSTRUCT(trigTup);

	/*
	 * Some of the elements only make sense for CONSTRAINT TRIGGERs, but it
	 * seems simpler to use a single fmt string for both kinds of triggers.
	 */
	trigger =
		new_objtree_VA("CREATE %{constraint}s TRIGGER %{name}I %{time}s %{events: OR }s "
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
	else if (node->timing == TRIGGER_TYPE_INSTEAD)
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
		events = lappend(events, new_string_object("INSERT"));
	if (node->events & TRIGGER_TYPE_DELETE)
		events = lappend(events, new_string_object("DELETE"));
	if (node->events & TRIGGER_TYPE_TRUNCATE)
		events = lappend(events, new_string_object("TRUNCATE"));
	if (node->events & TRIGGER_TYPE_UPDATE)
	{
		if (node->columns == NIL)
		{
			events = lappend(events, new_string_object("UPDATE"));
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
			update = new_objtree_VA("UPDATE OF %{columns:, }I",
									1, "kind", ObjTypeString, "update_of");

			foreach(cell, node->columns)
			{
				char   *colname = strVal(lfirst(cell));

				cols = lappend(cols,
							   new_string_object(colname));
			}

			append_array_object(update, "columns", cols);

			events = lappend(events,
							 new_object_object(update));
		}
	}
	append_array_object(trigger, "events", events);

	tmp = new_objtree_for_qualname_id(RelationRelationId,
									  trigForm->tgrelid);
	append_object_object(trigger, "relation", tmp);

	tmp = new_objtree_VA("FROM %{relation}D", 0);
	if (trigForm->tgconstrrelid)
	{
		ObjTree	   *rel;

		rel = new_objtree_for_qualname_id(RelationRelationId,
										  trigForm->tgconstrrelid);
		append_object_object(tmp, "relation", rel);
	}
	else
		append_bool_object(tmp, "present", false);
	append_object_object(trigger, "from_table", tmp);

	list = NIL;
	if (node->deferrable)
		list = lappend(list,
					   new_string_object("DEFERRABLE"));
	if (node->initdeferred)
		list = lappend(list,
					   new_string_object("INITIALLY DEFERRED"));
	append_array_object(trigger, "constraint_attrs", list);

	append_string_object(trigger, "for_each",
						 node->row ? "ROW" : "STATEMENT");

	tmp = new_objtree_VA("WHEN (%{clause}s)", 0);
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

	tmp = new_objtree_VA("%{funcname}D(%{args:, }L)",
						 1, "funcname", ObjTypeObject,
						 new_objtree_for_qualname_id(ProcedureRelationId,
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

			list = lappend(list, new_string_object(p));

			p += tlen + 1;
		}
	}

	append_array_object(tmp, "args", list);		/* might be NIL */

	append_object_object(trigger, "function", tmp);

	heap_close(pg_trigger, AccessShareLock);

	return trigger;
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
static ObjTree *
deparse_ColumnDef(Relation relation, List *dpcontext, bool composite,
				  ColumnDef *coldef)
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
		column = new_objtree_VA("%{name}I %{coltype}T %{collation}s",
								3,
								"type", ObjTypeString, "column",
								"name", ObjTypeString, coldef->colname,
								"coltype", ObjTypeObject,
								new_objtree_for_type(typid, typmod));
	else
		column = new_objtree_VA("%{name}I %{coltype}T %{default}s %{not_null}s %{collation}s",
								3,
								"type", ObjTypeString, "column",
								"name", ObjTypeString, coldef->colname,
								"coltype", ObjTypeObject,
								new_objtree_for_type(typid, typmod));

	tmp = new_objtree_VA("COLLATE %{name}D", 0);
	if (OidIsValid(typcollation))
	{
		ObjTree *collname;

		collname = new_objtree_for_qualname_id(CollationRelationId,
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

		tmp = new_objtree_VA("DEFAULT %{default}s", 0);
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

	return column;
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
static ObjTree *
deparse_ColumnDef_typed(Relation relation, List *dpcontext, ColumnDef *coldef)
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
		column = new_objtree_VA("%{name}I WITH OPTIONS NOT NULL", 2,
								"type", ObjTypeString, "column_notnull",
								"name", ObjTypeString, coldef->colname);

	ReleaseSysCache(attrTup);

	return column;
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
deparseTableElements(Relation relation, List *tableElements, List *dpcontext,
					 bool typed, bool composite)
{
	List	   *elements = NIL;
	ListCell   *lc;

	foreach(lc, tableElements)
	{
		Node	   *elt = (Node *) lfirst(lc);

		switch (nodeTag(elt))
		{
			case T_ColumnDef:
				{
					ObjTree	   *tree;

					tree = typed ?
						deparse_ColumnDef_typed(relation, dpcontext,
												(ColumnDef *) elt) :
						deparse_ColumnDef(relation, dpcontext,
										  composite, (ColumnDef *) elt);
					if (tree != NULL)
					{
						ObjElem    *column;

						column = new_object_object(tree);
						elements = lappend(elements, column);
					}
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
 * obtainConstraints
 *		Subroutine for CREATE TABLE/CREATE DOMAIN deparsing
 *
 * Given a table OID or domain OID, obtain its constraints and append them to
 * the given elements list.  The updated list is returned.
 *
 * This works for typed tables, regular tables, and domains.
 *
 * Note that CONSTRAINT_FOREIGN constraints are always ignored.
 */
static List *
obtainConstraints(List *elements, Oid relationId, Oid domainId)
{
	Relation	conRel;
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	tuple;
	ObjTree    *tmp;

	/* only one may be valid */
	Assert(OidIsValid(relationId) ^ OidIsValid(domainId));

	/*
	 * scan pg_constraint to fetch all constraints linked to the given
	 * relation.
	 */
	conRel = heap_open(ConstraintRelationId, AccessShareLock);
	if (OidIsValid(relationId))
	{
		ScanKeyInit(&key,
					Anum_pg_constraint_conrelid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(relationId));
		scan = systable_beginscan(conRel, ConstraintRelidIndexId,
								  true, NULL, 1, &key);
	}
	else
	{
		Assert(OidIsValid(domainId));
		ScanKeyInit(&key,
					Anum_pg_constraint_contypid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(domainId));
		scan = systable_beginscan(conRel, ConstraintTypidIndexId,
								  true, NULL, 1, &key);
	}

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
				continue;	/* not here */
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
		tmp = new_objtree_VA("CONSTRAINT %{name}I %{definition}s",
							 4,
							 "type", ObjTypeString, "constraint",
							 "contype", ObjTypeString, contype,
						 "name", ObjTypeString, NameStr(constrForm->conname),
							 "definition", ObjTypeString,
						  pg_get_constraintdef_string(HeapTupleGetOid(tuple),
													  false));
		elements = lappend(elements, new_object_object(tmp));
	}

	systable_endscan(scan);
	heap_close(conRel, AccessShareLock);

	return elements;
}

/*
 * deparse the ON COMMMIT ... clause for CREATE ... TEMPORARY ...
 */
static ObjTree *
deparse_OnCommitClause(OnCommitAction option)
{
	ObjTree	   *tmp;

	tmp = new_objtree_VA("ON COMMIT %{on_commit_value}s", 0);
	switch (option)
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

	return tmp;
}

/*
 * Deparse DefElems, as used e.g. by ALTER COLUMN ... SET, into a list of SET
 * (...)  or RESET (...) contents.
 */
static ObjTree *
deparse_DefElem(DefElem *elem, bool is_reset)
{
	ObjTree	   *set;
	ObjTree	   *optname;

	if (elem->defnamespace != NULL)
		optname = new_objtree_VA("%{schema}I.%{label}I", 1,
								 "schema", ObjTypeString, elem->defnamespace);
	else
		optname = new_objtree_VA("%{label}I", 0);

	append_string_object(optname, "label", elem->defname);

	if (is_reset)
		set = new_objtree_VA("%{label}s", 0);
	else
		set = new_objtree_VA("%{label}s = %{value}L", 1,
							 "value", ObjTypeString,
							 elem->arg ? defGetString(elem) :
							 defGetBoolean(elem) ? "TRUE" : "FALSE");

	append_object_object(set, "label", optname);
	return set;
}

/*
 * deparse_CreateStmt
 *		Deparse a CreateStmt (CREATE TABLE)
 *
 * Given a table OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 */
static ObjTree *
deparse_CreateStmt(Oid objectId, Node *parsetree)
{
	CreateStmt *node = (CreateStmt *) parsetree;
	Relation	relation = relation_open(objectId, AccessShareLock);
	List	   *dpcontext;
	ObjTree    *createStmt;
	ObjTree    *tmp;
	List	   *list;
	ListCell   *cell;
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
			"WITH (%{with:, }s) %{on_commit}s %{tablespace}s";
	else
		fmtstr = "CREATE %{persistence}s TABLE %{if_not_exists}s %{identity}D "
			"(%{table_elements:, }s) %{inherits}s "
			"WITH (%{with:, }s) %{on_commit}s %{tablespace}s";

	createStmt =
		new_objtree_VA(fmtstr, 1,
					   "persistence", ObjTypeString,
					   get_persistence_str(relation->rd_rel->relpersistence));

	tmp = new_objtree_for_qualname(relation->rd_rel->relnamespace,
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

		tmp = new_objtree_for_type(relation->rd_rel->reloftype, -1);
		append_object_object(createStmt, "of_type", tmp);

		tableelts = deparseTableElements(relation, node->tableElts, dpcontext,
										 true,		/* typed table */
										 false);	/* not composite */
		tableelts = obtainConstraints(tableelts, objectId, InvalidOid);
		if (tableelts == NIL)
			tmp = new_objtree_VA("", 1,
								 "present", ObjTypeBool, false);
		else
			tmp = new_objtree_VA("(%{elements:, }s)", 1,
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
		tableelts = deparseTableElements(relation, node->tableElts, dpcontext,
										 false,		/* not typed table */
										 false);	/* not composite */
		tableelts = obtainConstraints(tableelts, objectId, InvalidOid);

		append_array_object(createStmt, "table_elements", tableelts);

		/*
		 * Add inheritance specification.  We cannot simply scan the list of
		 * parents from the parser node, because that may lack the actual
		 * qualified names of the parent relations.  Rather than trying to
		 * re-resolve them from the information in the parse node, it seems
		 * more accurate and convenient to grab it from pg_inherits.
		 */
		tmp = new_objtree_VA("INHERITS (%{parents:, }D)", 0);
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

				parent = new_objtree_for_qualname_id(RelationRelationId,
													 formInh->inhparent);
				parents = lappend(parents, new_object_object(parent));
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

	tmp = new_objtree_VA("TABLESPACE %{tablespace}I", 0);
	if (node->tablespacename)
		append_string_object(tmp, "tablespace", node->tablespacename);
	else
	{
		append_null_object(tmp, "tablespace");
		append_bool_object(tmp, "present", false);
	}
	append_object_object(createStmt, "tablespace", tmp);

	append_object_object(createStmt, "on_commit",
						 deparse_OnCommitClause(node->oncommit));

	/*
	 * WITH clause.  We always emit one, containing at least the OIDS option.
	 * That way we don't depend on the default value for default_with_oids.
	 * We can skip emitting other options if there don't appear in the parse
	 * node.
	 */
	tmp = new_objtree_VA("oids=%{value}s", 2,
						 "option", ObjTypeString, "oids",
						 "value", ObjTypeString,
						 relation->rd_rel->relhasoids ? "ON" : "OFF");
	list = list_make1(new_object_object(tmp));

	foreach(cell, node->options)
	{
		DefElem	*opt = (DefElem *) lfirst(cell);

		/* already handled above */
		if (strcmp(opt->defname, "oids") == 0)
			continue;

		tmp = deparse_DefElem(opt, false);
		list = lappend(list, new_object_object(tmp));
	}
	append_array_object(createStmt, "with", list);

	relation_close(relation, AccessShareLock);

	return createStmt;
}

/*
 * deparse_CompositeTypeStmt
 *		Deparse a CompositeTypeStmt (CREATE TYPE AS)
 *
 * Given a type OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 */
static ObjTree *
deparse_CompositeTypeStmt(Oid objectId, Node *parsetree)
{
	CompositeTypeStmt *node = (CompositeTypeStmt *) parsetree;
	ObjTree	   *composite;
	HeapTuple	typtup;
	Form_pg_type typform;
	Relation	typerel;
	List	   *dpcontext;
	List	   *tableelts = NIL;

	/* Find the pg_type entry and open the corresponding relation */
	typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(typtup))
		elog(ERROR, "cache lookup failed for type %u", objectId);
	typform = (Form_pg_type) GETSTRUCT(typtup);
	typerel = relation_open(typform->typrelid, AccessShareLock);

	dpcontext = deparse_context_for(RelationGetRelationName(typerel),
									RelationGetRelid(typerel));

	composite = new_objtree_VA("CREATE TYPE %{identity}D AS (%{columns:, }s)",
							   0);
	append_object_object(composite, "identity",
						 new_objtree_for_qualname_id(TypeRelationId,
													 objectId));

	tableelts = deparseTableElements(typerel, node->coldeflist, dpcontext,
									 false,		/* not typed */
									 true);		/* composite type */

	append_array_object(composite, "columns", tableelts);

	heap_close(typerel, AccessShareLock);
	ReleaseSysCache(typtup);

	return composite;
}

/*
 * deparse_CreateEnumStmt
 *		Deparse a CreateEnumStmt (CREATE TYPE AS ENUM)
 *
 * Given a type OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 */
static ObjTree *
deparse_CreateEnumStmt(Oid objectId, Node *parsetree)
{
	CreateEnumStmt *node = (CreateEnumStmt *) parsetree;
	ObjTree	   *enumtype;
	List	   *values;
	ListCell   *cell;

	enumtype = new_objtree_VA("CREATE TYPE %{identity}D AS ENUM (%{values:, }L)",
							  0);
	append_object_object(enumtype, "identity",
						 new_objtree_for_qualname_id(TypeRelationId,
													 objectId));
	values = NIL;
	foreach(cell, node->vals)
	{
		Value   *val = (Value *) lfirst(cell);

		values = lappend(values, new_string_object(strVal(val)));
	}
	append_array_object(enumtype, "values", values);

	return enumtype;
}

static ObjTree *
deparse_CreateRangeStmt(Oid objectId, Node *parsetree)
{
	ObjTree	   *range;
	ObjTree	   *tmp;
	List	   *definition = NIL;
	Relation	pg_range;
	HeapTuple	rangeTup;
	Form_pg_range rangeForm;
	ScanKeyData key[1];
	SysScanDesc scan;

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

	range = new_objtree_VA("CREATE TYPE %{identity}D AS RANGE (%{definition:, }s)", 0);
	tmp = new_objtree_for_qualname_id(TypeRelationId, objectId);
	append_object_object(range, "identity", tmp);

	/* SUBTYPE */
	tmp = new_objtree_for_qualname_id(TypeRelationId,
									  rangeForm->rngsubtype);
	tmp = new_objtree_VA("SUBTYPE = %{type}D",
						 2,
						 "clause", ObjTypeString, "subtype",
						 "type", ObjTypeObject, tmp);
	definition = lappend(definition, new_object_object(tmp));

	/* SUBTYPE_OPCLASS */
	if (OidIsValid(rangeForm->rngsubopc))
	{
		tmp = new_objtree_for_qualname_id(OperatorClassRelationId,
										  rangeForm->rngsubopc);
		tmp = new_objtree_VA("SUBTYPE_OPCLASS = %{opclass}D",
							 2,
							 "clause", ObjTypeString, "opclass",
							 "opclass", ObjTypeObject, tmp);
		definition = lappend(definition, new_object_object(tmp));
	}

	/* COLLATION */
	if (OidIsValid(rangeForm->rngcollation))
	{
		tmp = new_objtree_for_qualname_id(CollationRelationId,
										  rangeForm->rngcollation);
		tmp = new_objtree_VA("COLLATION = %{collation}D",
							 2,
							 "clause", ObjTypeString, "collation",
							 "collation", ObjTypeObject, tmp);
		definition = lappend(definition, new_object_object(tmp));
	}

	/* CANONICAL */
	if (OidIsValid(rangeForm->rngcanonical))
	{
		tmp = new_objtree_for_qualname_id(ProcedureRelationId,
										  rangeForm->rngcanonical);
		tmp = new_objtree_VA("CANONICAL = %{canonical}D",
							 2,
							 "clause", ObjTypeString, "canonical",
							 "canonical", ObjTypeObject, tmp);
		definition = lappend(definition, new_object_object(tmp));
	}

	/* SUBTYPE_DIFF */
	if (OidIsValid(rangeForm->rngsubdiff))
	{
		tmp = new_objtree_for_qualname_id(ProcedureRelationId,
										  rangeForm->rngsubdiff);
		tmp = new_objtree_VA("SUBTYPE_DIFF = %{subtype_diff}D",
							 2,
							 "clause", ObjTypeString, "subtype_diff",
							 "subtype_diff", ObjTypeObject, tmp);
		definition = lappend(definition, new_object_object(tmp));
	}

	append_array_object(range, "definition", definition);

	systable_endscan(scan);
	heap_close(pg_range, RowExclusiveLock);

	return range;
}

static ObjTree *
deparse_CreateDomain(Oid objectId, Node *parsetree)
{
	ObjTree	   *createDomain;
	ObjTree	   *tmp;
	HeapTuple	typTup;
	Form_pg_type typForm;
	List	   *constraints;

	typTup = SearchSysCache1(TYPEOID,
							 objectId);
	if (!HeapTupleIsValid(typTup))
		elog(ERROR, "cache lookup failed for domain with OID %u", objectId);
	typForm = (Form_pg_type) GETSTRUCT(typTup);

	createDomain = new_objtree_VA("CREATE DOMAIN %{identity}D AS %{type}T %{not_null}s %{constraints}s %{collation}s",
								  0);

	append_object_object(createDomain,
						 "identity",
						 new_objtree_for_qualname_id(TypeRelationId,
													 objectId));
	append_object_object(createDomain,
						 "type",
						 new_objtree_for_type(typForm->typbasetype, typForm->typtypmod));

	if (typForm->typnotnull)
		append_string_object(createDomain, "not_null", "NOT NULL");
	else
		append_string_object(createDomain, "not_null", "");

	constraints = obtainConstraints(NIL, InvalidOid, objectId);
	tmp = new_objtree_VA("%{elements: }s", 0);
	if (constraints == NIL)
		append_bool_object(tmp, "present", false);
	else
		append_array_object(tmp, "elements", constraints);
	append_object_object(createDomain, "constraints", tmp);

	tmp = new_objtree_VA("COLLATE %{collation}D", 0);
	if (OidIsValid(typForm->typcollation))
		append_object_object(tmp, "collation",
							 new_objtree_for_qualname_id(CollationRelationId,
														 typForm->typcollation));
	else
		append_bool_object(tmp, "present", false);
	append_object_object(createDomain, "collation", tmp);

	ReleaseSysCache(typTup);

	return createDomain;
}

static ObjTree *
deparse_FunctionSet(VariableSetKind kind, char *name, char *value)
{
	ObjTree	   *r;

	if (kind == VAR_RESET_ALL)
	{
		r = new_objtree_VA("RESET ALL", 0);
	}
	else if (value != NULL)
	{
		/*
		 * Some GUC variable names are 'LIST' type and hence must not be
		 * quoted. FIXME: shouldn't this and pg_get_functiondef() rather use
		 * guc.c to check for GUC_LIST?
		 */
		if (pg_strcasecmp(name, "DateStyle") == 0
			|| pg_strcasecmp(name, "search_path") == 0)
			r = new_objtree_VA("SET %{set_name}I TO %{set_value}s", 0);
		else
			r = new_objtree_VA("SET %{set_name}I TO %{set_value}L", 0);

		append_string_object(r, "set_name", name);
		append_string_object(r, "set_value", value);
	}
	else
	{
		r = new_objtree_VA("RESET %{set_name}I", 0);
		append_string_object(r, "set_name", name);
	}

	return r;
}

/*
 * deparse_CreateFunctionStmt
 *		Deparse a CreateFunctionStmt (CREATE FUNCTION)
 *
 * Given a function OID and the parsetree that created it, return the JSON
 * blob representing the creation command.
 */
static ObjTree *
deparse_CreateFunction(Oid objectId, Node *parsetree)
{
	CreateFunctionStmt *node = (CreateFunctionStmt *) parsetree;
	ObjTree	   *createFunc;
	ObjTree	   *sign;
	ObjTree	   *tmp;
	Datum		tmpdatum;
	char	   *fmt;
	char	   *definition;
	char	   *source;
	char	   *probin;
	List	   *params;
	List	   *defaults;
	List	   *sets = NIL;
	ListCell   *cell;
	ListCell   *curdef;
	ListCell   *table_params = NULL;
	HeapTuple	procTup;
	Form_pg_proc procForm;
	HeapTuple	langTup;
	Oid		   *typarray;
	Form_pg_language langForm;
	int			i;
	int			typnum;
	bool		isnull;

	/* get the pg_proc tuple */
	procTup = SearchSysCache1(PROCOID, objectId);
	if (!HeapTupleIsValid(procTup))
		elog(ERROR, "cache lookup failure for function with OID %u",
			 objectId);
	procForm = (Form_pg_proc) GETSTRUCT(procTup);

	/* get the corresponding pg_language tuple */
	langTup = SearchSysCache1(LANGOID, procForm->prolang);
	if (!HeapTupleIsValid(langTup))
		elog(ERROR, "cache lookup failure for language with OID %u",
			 procForm->prolang);
	langForm = (Form_pg_language) GETSTRUCT(langTup);

	/*
	 * Determine useful values for prosrc and probin.  We cope with probin
	 * being either NULL or "-", but prosrc must have a valid value.
	 */
	tmpdatum = SysCacheGetAttr(PROCOID, procTup,
							   Anum_pg_proc_prosrc, &isnull);
	if (isnull)
		elog(ERROR, "null prosrc in function with OID %u", objectId);
	source = TextDatumGetCString(tmpdatum);

	/* Determine a useful value for probin */
	tmpdatum = SysCacheGetAttr(PROCOID, procTup,
							   Anum_pg_proc_probin, &isnull);
	if (isnull)
		probin = NULL;
	else
	{
		probin = TextDatumGetCString(tmpdatum);
		if (probin[0] == '\0' || strcmp(probin, "-") == 0)
			probin = NULL;
	}

	if (probin == NULL)
		definition = "%{definition}L";
	else
		definition = "%{objfile}L, %{symbol}L";

	fmt = psprintf("CREATE %%{or_replace}s FUNCTION %%{signature}s "
				   "RETURNS %%{return_type}s LANGUAGE %%{language}I "
				   "%%{window}s %%{volatility}s %%{leakproof}s "
				   "%%{strict}s %%{security_definer}s %%{cost}s %%{rows}s "
				   "%%{set_options: }s "
				   "AS %s", definition);

	createFunc = new_objtree_VA(fmt, 1,
								"or_replace", ObjTypeString,
								node->replace ? "OR REPLACE" : "");

	sign = new_objtree_VA("%{identity}D(%{arguments:, }s)", 0);

	/*
	 * To construct the arguments array, extract the type OIDs from the
	 * function's pg_proc entry.  If pronargs equals the parameter list length,
	 * there are no OUT parameters and thus we can extract the type OID from
	 * proargtypes; otherwise we need to decode proallargtypes, which is
	 * a bit more involved.
	 */
	typarray = palloc(list_length(node->parameters) * sizeof(Oid));
	if (list_length(node->parameters) > procForm->pronargs)
	{
		bool	isnull;
		Datum	alltypes;
		Datum  *values;
		bool   *nulls;
		int		nelems;

		alltypes = SysCacheGetAttr(PROCOID, procTup,
								   Anum_pg_proc_proallargtypes, &isnull);
		if (isnull)
			elog(ERROR, "NULL proallargtypes, but more parameters than args");
		deconstruct_array(DatumGetArrayTypeP(alltypes),
						  OIDOID, 4, 't', 'i',
						  &values, &nulls, &nelems);
		if (nelems != list_length(node->parameters))
			elog(ERROR, "mismatched proallargatypes");
		for (i = 0; i < list_length(node->parameters); i++)
			typarray[i] = values[i];
	}
	else
	{
		for (i = 0; i < list_length(node->parameters); i++)
			 typarray[i] = procForm->proargtypes.values[i];
	}

	/*
	 * If there are any default expressions, we read the deparsed expression as
	 * a list so that we can attach them to each argument.
	 */
	tmpdatum = SysCacheGetAttr(PROCOID, procTup,
							   Anum_pg_proc_proargdefaults, &isnull);
	if (!isnull)
	{
		defaults = FunctionGetDefaults(DatumGetTextP(tmpdatum));
		curdef = list_head(defaults);
	}
	else
	{
		defaults = NIL;
		curdef = NULL;
	}

	/*
	 * Now iterate over each parameter in the parsetree to create the
	 * parameters array.
	 */
	params = NIL;
	typnum = 0;
	foreach(cell, node->parameters)
	{
		FunctionParameter *param = (FunctionParameter *) lfirst(cell);
		ObjTree	   *tmp2;
		ObjTree	   *tmp3;

		/*
		 * A PARAM_TABLE parameter indicates end of input arguments; the
		 * following parameters are part of the return type.  We ignore them
		 * here, but keep track of the current position in the list so that
		 * we can easily produce the return type below.
		 */
		if (param->mode == FUNC_PARAM_TABLE)
		{
			table_params = cell;
			break;
		}

		/*
		 * Note that %{name}s is a string here, not an identifier; the reason
		 * for this is that an absent parameter name must produce an empty
		 * string, not "", which is what would happen if we were to use
		 * %{name}I here.  So we add another level of indirection to allow us
		 * to inject a "present" parameter.
		 */
		tmp2 = new_objtree_VA("%{mode}s %{name}s %{type}T %{default}s", 0);
		append_string_object(tmp2, "mode",
							 param->mode == FUNC_PARAM_IN ? "IN" :
							 param->mode == FUNC_PARAM_OUT ? "OUT" :
							 param->mode == FUNC_PARAM_INOUT ? "INOUT" :
							 param->mode == FUNC_PARAM_VARIADIC ? "VARIADIC" :
							 "INVALID MODE");

		/* optional wholesale suppression of "name" occurs here */
		append_object_object(tmp2, "name",
							 new_objtree_VA("%{name}I", 2,
											"name", ObjTypeString,
											param->name ? param->name : "NULL",
											"present", ObjTypeBool,
											param->name ? true : false));

		tmp3 = new_objtree_VA("DEFAULT %{value}s", 0);
		if (PointerIsValid(param->defexpr))
		{
			char *expr;

			if (curdef == NULL)
				elog(ERROR, "proargdefaults list too short");
			expr = lfirst(curdef);

			append_string_object(tmp3, "value", expr);
			curdef = lnext(curdef);
		}
		else
			append_bool_object(tmp3, "present", false);
		append_object_object(tmp2, "default", tmp3);

		append_object_object(tmp2, "type",
							 new_objtree_for_type(typarray[typnum++], -1));

		params = lappend(params, new_object_object(tmp2));
	}
	append_array_object(sign, "arguments", params);
	append_object_object(sign, "identity",
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 objectId));
	append_object_object(createFunc, "signature", sign);

	/*
	 * A return type can adopt one of two forms: either a [SETOF] some_type, or
	 * a TABLE(list-of-types).  We can tell the second form because we saw a
	 * table param above while scanning the argument list.
	 */
	if (table_params == NULL)
	{
		tmp = new_objtree_VA("%{setof}s %{rettype}T", 0);
		append_string_object(tmp, "setof",
							 procForm->proretset ? "SETOF" : "");
		append_object_object(tmp, "rettype",
							 new_objtree_for_type(procForm->prorettype, -1));
		append_string_object(tmp, "return_form", "plain");
	}
	else
	{
		List	   *rettypes = NIL;
		ObjTree	   *tmp2;

		tmp = new_objtree_VA("TABLE (%{rettypes:, }s)", 0);
		for (; table_params != NULL; table_params = lnext(table_params))
		{
			FunctionParameter *param = lfirst(table_params);

			tmp2 = new_objtree_VA("%{name}I %{type}T", 0);
			append_string_object(tmp2, "name", param->name);
			append_object_object(tmp2, "type",
								 new_objtree_for_type(typarray[typnum++], -1));
			rettypes = lappend(rettypes, new_object_object(tmp2));
		}

		append_array_object(tmp, "rettypes", rettypes);
		append_string_object(tmp, "return_form", "table");
	}

	append_object_object(createFunc, "return_type", tmp);

	append_string_object(createFunc, "language",
						 NameStr(langForm->lanname));

	append_string_object(createFunc, "window",
						 procForm->proiswindow ? "WINDOW" : "");
	append_string_object(createFunc, "volatility",
						 procForm->provolatile == PROVOLATILE_VOLATILE ?
						 "VOLATILE" :
						 procForm->provolatile == PROVOLATILE_STABLE ?
						 "STABLE" :
						 procForm->provolatile == PROVOLATILE_IMMUTABLE ?
						 "IMMUTABLE" : "INVALID VOLATILITY");

	append_string_object(createFunc, "leakproof",
						 procForm->proleakproof ? "LEAKPROOF" : "");
	append_string_object(createFunc, "strict",
						 procForm->proisstrict ?
						 "RETURNS NULL ON NULL INPUT" :
						 "CALLED ON NULL INPUT");

	append_string_object(createFunc, "security_definer",
						 procForm->prosecdef ?
						 "SECURITY DEFINER" : "SECURITY INVOKER");

	append_object_object(createFunc, "cost",
						 new_objtree_VA("COST %{cost}n", 1,
										"cost", ObjTypeFloat,
										procForm->procost));

	tmp = new_objtree_VA("ROWS %{rows}n", 0);
	if (procForm->prorows == 0)
		append_bool_object(tmp, "present", false);
	else
		append_float_object(tmp, "rows", procForm->prorows);
	append_object_object(createFunc, "rows", tmp);

	foreach(cell, node->options)
	{
		DefElem	*defel = (DefElem *) lfirst(cell);
		ObjTree	   *tmp = NULL;

		if (strcmp(defel->defname, "set") == 0)
		{
			VariableSetStmt *sstmt = (VariableSetStmt *) defel->arg;
			char *value = ExtractSetVariableArgs(sstmt);

			tmp = deparse_FunctionSet(sstmt->kind, sstmt->name, value);
			sets = lappend(sets, new_object_object(tmp));
		}
	}
	append_array_object(createFunc, "set_options", sets);

	if (probin == NULL)
	{
		append_string_object(createFunc, "definition",
							 source);
	}
	else
	{
		append_string_object(createFunc, "objfile", probin);
		append_string_object(createFunc, "symbol", source);
	}

	ReleaseSysCache(langTup);
	ReleaseSysCache(procTup);

	return createFunc;
}

/*
 * deparse_AlterFunctionStmt
 *		Deparse a AlterFunctionStmt (ALTER FUNCTION)
 *
 * Given a function OID and the parsetree that created it, return the JSON
 * blob representing the alter command.
 */
static ObjTree *
deparse_AlterFunction(Oid objectId, Node *parsetree)
{
	AlterFunctionStmt *node = (AlterFunctionStmt *) parsetree;
	ObjTree	   *alterFunc;
	ObjTree	   *sign;
	HeapTuple	procTup;
	Form_pg_proc procForm;
	List	   *params;
	List	   *elems = NIL;
	ListCell   *cell;
	int			i;

	/* get the pg_proc tuple */
	procTup = SearchSysCache1(PROCOID, objectId);
	if (!HeapTupleIsValid(procTup))
		elog(ERROR, "cache lookup failure for function with OID %u",
			 objectId);
	procForm = (Form_pg_proc) GETSTRUCT(procTup);

	alterFunc = new_objtree_VA("ALTER FUNCTION %{signature}s %{definition: }s", 0);

	sign = new_objtree_VA("%{identity}D(%{arguments:, }s)", 0);

	params = NIL;

	/*
	 * ALTER FUNCTION does not change signature so we can use catalog
	 * to get input type Oids.
	 */
	for (i = 0; i < procForm->pronargs; i++)
	{
		ObjTree	   *tmp = new_objtree_VA("%{type}T", 0);

		append_object_object(tmp, "type",
							 new_objtree_for_type(procForm->proargtypes.values[i], -1));
		params = lappend(params, new_object_object(tmp));
	}

	append_array_object(sign, "arguments", params);
	append_object_object(sign, "identity",
						 new_objtree_for_qualname_id(ProcedureRelationId,
													 objectId));
	append_object_object(alterFunc, "signature", sign);

	foreach(cell, node->actions)
	{
		DefElem	*defel = (DefElem *) lfirst(cell);
		ObjTree	   *tmp = NULL;

		if (strcmp(defel->defname, "volatility") == 0)
		{
			tmp = new_objtree_VA(strVal(defel->arg), 0);
		}
		else if (strcmp(defel->defname, "strict") == 0)
		{
			tmp = new_objtree_VA(intVal(defel->arg) ?
								 "RETURNS NULL ON NULL INPUT" :
								 "CALLED ON NULL INPUT", 0);
		}
		else if (strcmp(defel->defname, "security") == 0)
		{
			tmp = new_objtree_VA(intVal(defel->arg) ?
								 "SECURITY DEFINER" : "SECURITY INVOKER", 0);
		}
		else if (strcmp(defel->defname, "leakproof") == 0)
		{
			tmp = new_objtree_VA(intVal(defel->arg) ?
								 "LEAKPROOF" : "NOT LEAKPROOF", 0);
		}
		else if (strcmp(defel->defname, "cost") == 0)
		{
			tmp = new_objtree_VA("COST %{cost}n", 1,
								 "cost", ObjTypeFloat,
								 defGetNumeric(defel));
		}
		else if (strcmp(defel->defname, "rows") == 0)
		{
			tmp = new_objtree_VA("ROWS %{rows}n", 0);
			if (defGetNumeric(defel) == 0)
				append_bool_object(tmp, "present", false);
			else
				append_float_object(tmp, "rows",
									defGetNumeric(defel));
		}
		else if (strcmp(defel->defname, "set") == 0)
		{
			VariableSetStmt *sstmt = (VariableSetStmt *) defel->arg;
			char *value = ExtractSetVariableArgs(sstmt);

			tmp = deparse_FunctionSet(sstmt->kind, sstmt->name, value);
		}

		elems = lappend(elems, new_object_object(tmp));
	}

	append_array_object(alterFunc, "definition", elems);

	ReleaseSysCache(procTup);

	return alterFunc;
}

/*
 * Return the given object type as a string.
 */
static const char *
stringify_objtype(ObjectType objtype)
{
	switch (objtype)
	{
		case OBJECT_AGGREGATE:
			return "AGGREGATE";
		case OBJECT_CAST:
			return "CAST";
		case OBJECT_COLUMN:
			return "COLUMN";
		case OBJECT_COLLATION:
			return "COLLATION";
		case OBJECT_CONVERSION:
			return "CONVERSION";
		case OBJECT_DATABASE:
			return "DATABASE";
		case OBJECT_DOMAIN:
			return "DOMAIN";
		case OBJECT_EVENT_TRIGGER:
			return "EVENT TRIGGER";
		case OBJECT_EXTENSION:
			return "EXTENSION";
		case OBJECT_FDW:
			return "FOREIGN DATA WRAPPER";
		case OBJECT_FOREIGN_SERVER:
			return "SERVER";
		case OBJECT_FOREIGN_TABLE:
			return "FOREIGN TABLE";
		case OBJECT_FUNCTION:
			return "FUNCTION";
		case OBJECT_INDEX:
			return "INDEX";
		case OBJECT_LANGUAGE:
			return "LANGUAGE";
		case OBJECT_LARGEOBJECT:
			return "LARGE OBJECT";
		case OBJECT_MATVIEW:
			return "MATERIALIZED VIEW";
		case OBJECT_OPCLASS:
			return "OPERATOR CLASS";
		case OBJECT_OPERATOR:
			return "OPERATOR";
		case OBJECT_OPFAMILY:
			return "OPERATOR FAMILY";
		case OBJECT_ROLE:
			return "ROLE";
		case OBJECT_RULE:
			return "RULE";
		case OBJECT_SCHEMA:
			return "SCHEMA";
		case OBJECT_SEQUENCE:
			return "SEQUENCE";
		case OBJECT_TABLE:
			return "TABLE";
		case OBJECT_TABLESPACE:
			return "TABLESPACE";
		case OBJECT_TRIGGER:
			return "TRIGGER";
		case OBJECT_TSCONFIGURATION:
			return "TEXT SEARCH CONFIGURATION";
		case OBJECT_TSDICTIONARY:
			return "TEXT SEARCH DICTIONARY";
		case OBJECT_TSPARSER:
			return "TEXT SEARCH PARSER";
		case OBJECT_TSTEMPLATE:
			return "TEXT SEARCH TEMPLATE";
		case OBJECT_TYPE:
			return "TYPE";
		case OBJECT_USER_MAPPING:
			return "USER MAPPING";
		case OBJECT_VIEW:
			return "VIEW";

		default:
			elog(ERROR, "unsupported object type %d", objtype);
	}
}

static ObjTree *
deparse_RenameStmt(ObjectAddress address, Node *parsetree)
{
	RenameStmt *node = (RenameStmt *) parsetree;
	ObjTree	   *renameStmt;
	char	   *fmtstr;
	Relation	relation;
	Oid			schemaId;

	/*
	 * FIXME --- this code is missing support for inheritance behavioral flags,
	 * i.e. the "*" and ONLY elements.
	 */

	/*
	 * In a ALTER .. RENAME command, we don't have the original name of the
	 * object in system catalogs: since we inspect them after the command has
	 * executed, the old name is already gone.  Therefore, we extract it from
	 * the parse node.  Note we still extract the schema name from the catalog
	 * (it might not be present in the parse node); it cannot possibly have
	 * changed anyway.
	 *
	 * XXX what if there's another event trigger running concurrently that
	 * renames the schema or moves the object to another schema?  Seems
	 * pretty far-fetched, but possible nonetheless.
	 */
	switch (node->renameType)
	{
		case OBJECT_TABLE:
		case OBJECT_SEQUENCE:
		case OBJECT_VIEW:
		case OBJECT_MATVIEW:
		case OBJECT_INDEX:
		case OBJECT_FOREIGN_TABLE:
			fmtstr = psprintf("ALTER %s %%{if_exists}s %%{identity}D RENAME TO %%{newname}I",
							  stringify_objtype(node->renameType));
			relation = relation_open(address.objectId, AccessShareLock);
			schemaId = RelationGetNamespace(relation);
			renameStmt = new_objtree_VA(fmtstr, 0);
			append_object_object(renameStmt, "identity",
								 new_objtree_for_qualname(schemaId,
														  node->relation->relname));
			append_string_object(renameStmt, "if_exists",
								 node->missing_ok ? "IF EXISTS" : "");
			relation_close(relation, AccessShareLock);
			break;

		case OBJECT_ATTRIBUTE:
		case OBJECT_COLUMN:
			relation = relation_open(address.objectId, AccessShareLock);
			schemaId = RelationGetNamespace(relation);

			if (node->renameType == OBJECT_ATTRIBUTE)
			{
				fmtstr = "ALTER TYPE %{identity}D RENAME ATTRIBUTE %{colname}I TO %{newname}I %{cascade}s";
				renameStmt = new_objtree_VA(fmtstr, 0);
				append_object_object(renameStmt, "cascade",
									 new_objtree_VA("CASCADE", 1,
													"present", ObjTypeBool,
													node->behavior == DROP_CASCADE));
			}
			else
			{
				fmtstr = psprintf("ALTER %s %%{if_exists}s %%{identity}D RENAME COLUMN %%{colname}I TO %%{newname}I",
								  stringify_objtype(node->relationType));
				renameStmt = new_objtree_VA(fmtstr, 0);
			}

			append_object_object(renameStmt, "identity",
								 new_objtree_for_qualname(schemaId,
														  node->relation->relname));
			append_string_object(renameStmt, "colname", node->subname);

			/* composite types do not support IF EXISTS */
			if (node->relationType != OBJECT_TYPE)
				append_string_object(renameStmt, "if_exists",
									 node->missing_ok ? "IF EXISTS" : "");
			relation_close(relation, AccessShareLock);

			break;

		case OBJECT_SCHEMA:
			{
				renameStmt =
					new_objtree_VA("ALTER SCHEMA %{identity}I RENAME TO %{newname}I",
								   0);
				append_string_object(renameStmt, "identity", node->subname);
			}
			break;

		case OBJECT_FDW:
		case OBJECT_LANGUAGE:
		case OBJECT_FOREIGN_SERVER:
			{
				fmtstr = psprintf("ALTER %s %%{identity}I RENAME TO %%{newname}I",
								  stringify_objtype(node->renameType));
				renameStmt = new_objtree_VA(fmtstr, 0);
				append_string_object(renameStmt, "identity",
									 strVal(linitial(node->object)));
			}
			break;

		case OBJECT_COLLATION:
		case OBJECT_CONVERSION:
		case OBJECT_DOMAIN:
		case OBJECT_TSDICTIONARY:
		case OBJECT_TSPARSER:
		case OBJECT_TSTEMPLATE:
		case OBJECT_TSCONFIGURATION:
		case OBJECT_TYPE:
			{
				HeapTuple	objTup;
				Relation	catalog;
				Datum		objnsp;
				bool		isnull;
				AttrNumber	Anum_namespace;
				ObjTree	   *ident;

				/* obtain object tuple */
				catalog = relation_open(address.classId, AccessShareLock);
				objTup = get_catalog_object_by_oid(catalog, address.objectId);

				/* obtain namespace */
				Anum_namespace = get_object_attnum_namespace(address.classId);
				objnsp = heap_getattr(objTup, Anum_namespace,
									  RelationGetDescr(catalog), &isnull);
				if (isnull)
					elog(ERROR, "invalid NULL namespace");

				fmtstr = psprintf("ALTER %s %%{identity}D RENAME TO %%{newname}I",
								  stringify_objtype(node->renameType));
				renameStmt = new_objtree_VA(fmtstr, 0);
				ident = new_objtree_for_qualname(DatumGetObjectId(objnsp),
												 strVal(llast(node->object)));
				append_object_object(renameStmt, "identity", ident);

				relation_close(catalog, AccessShareLock);
			}
			break;

		case OBJECT_OPCLASS:
		case OBJECT_OPFAMILY:
			{
				HeapTuple	objTup;
				HeapTuple	amTup;
				Relation	catalog;
				Datum		objnsp;
				bool		isnull;
				AttrNumber	Anum_namespace;
				Oid			amoid;
				ObjTree	   *ident;

				/* obtain object tuple */
				catalog = relation_open(address.classId, AccessShareLock);
				objTup = get_catalog_object_by_oid(catalog, address.objectId);

				/* obtain namespace */
				Anum_namespace = get_object_attnum_namespace(address.classId);
				objnsp = heap_getattr(objTup, Anum_namespace,
									  RelationGetDescr(catalog), &isnull);
				if (isnull)
					elog(ERROR, "invalid NULL namespace");

				/* obtain AM tuple */
				if (node->renameType == OBJECT_OPCLASS)
					amoid = ((Form_pg_opclass) GETSTRUCT(objTup))->opcmethod;
				else
					amoid = ((Form_pg_opfamily) GETSTRUCT(objTup))->opfmethod;
				amTup = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
				if (!HeapTupleIsValid(amTup))
					elog(ERROR, "cache lookup failed for access method %u", amoid);


				fmtstr = psprintf("ALTER %s %%{identity}D USING %%{amname}I RENAME TO %%{newname}I",
								  stringify_objtype(node->renameType));
				renameStmt = new_objtree_VA(fmtstr, 0);

				/* add the identity clauses */
				ident = new_objtree_for_qualname(DatumGetObjectId(objnsp),
												 strVal(llast(node->object)));
				append_object_object(renameStmt, "identity", ident);
				append_string_object(renameStmt, "amname",
									 pstrdup(NameStr(((Form_pg_am) GETSTRUCT(amTup))->amname)));

				ReleaseSysCache(amTup);
				relation_close(catalog, AccessShareLock);
			}
			break;

		case OBJECT_AGGREGATE:
		case OBJECT_FUNCTION:
			{
				char	*ident;
				HeapTuple proctup;
				Form_pg_proc procform;

				proctup = SearchSysCache1(PROCOID,
										  ObjectIdGetDatum(address.objectId));
				if (!HeapTupleIsValid(proctup))
					elog(ERROR, "cache lookup failed for procedure %u",
						 address.objectId);
				procform = (Form_pg_proc) GETSTRUCT(proctup);

				/* XXX does this work for ordered-set aggregates? */
				ident = psprintf("%s%s",
								 quote_qualified_identifier(get_namespace_name(procform->pronamespace),
															strVal(llast(node->object))),
								 format_procedure_args(address.objectId, true));

				fmtstr = psprintf("ALTER %s %%{identity}s RENAME TO %%{newname}I",
								  stringify_objtype(node->renameType));
				renameStmt = new_objtree_VA(fmtstr, 1,
											"identity", ObjTypeString, ident);

				ReleaseSysCache(proctup);
			}
			break;

		case OBJECT_TABCONSTRAINT:
		case OBJECT_DOMCONSTRAINT:
			{
				HeapTuple		conTup;
				Form_pg_constraint	constrForm;
				ObjTree		   *ident;

				conTup = SearchSysCache1(CONSTROID, address.objectId);
				constrForm = (Form_pg_constraint) GETSTRUCT(conTup);

				if (node->renameType == OBJECT_TABCONSTRAINT)
				{
					fmtstr = "ALTER TABLE %{identity}D RENAME CONSTRAINT %{conname}I TO %{newname}I";
					ident = new_objtree_for_qualname_id(RelationRelationId,
														constrForm->conrelid);
				}
				else
				{
					fmtstr = "ALTER DOMAIN %{identity}D RENAME CONSTRAINT %{conname}I TO %{newname}I";
					ident = new_objtree_for_qualname_id(TypeRelationId,
														constrForm->contypid);
				}
				renameStmt = new_objtree_VA(fmtstr, 2,
											"conname", ObjTypeString, node->subname,
											"identity", ObjTypeObject, ident);
				ReleaseSysCache(conTup);
			}
			break;

		case OBJECT_RULE:
			{
				HeapTuple	rewrTup;
				Form_pg_rewrite rewrForm;
				Relation	pg_rewrite;

				pg_rewrite = relation_open(RewriteRelationId, AccessShareLock);
				rewrTup = get_catalog_object_by_oid(pg_rewrite, address.objectId);
				rewrForm = (Form_pg_rewrite) GETSTRUCT(rewrTup);

				renameStmt = new_objtree_VA("ALTER RULE %{rulename}I ON %{identity}D RENAME TO %{newname}I",
											0);
				append_string_object(renameStmt, "rulename", node->subname);
				append_object_object(renameStmt, "identity",
									 new_objtree_for_qualname_id(RelationRelationId,
																 rewrForm->ev_class));
				relation_close(pg_rewrite, AccessShareLock);
			}
			break;

		case OBJECT_TRIGGER:
			{
				HeapTuple	trigTup;
				Form_pg_trigger trigForm;
				Relation	pg_trigger;

				pg_trigger = relation_open(TriggerRelationId, AccessShareLock);
				trigTup = get_catalog_object_by_oid(pg_trigger, address.objectId);
				trigForm = (Form_pg_trigger) GETSTRUCT(trigTup);

				renameStmt = new_objtree_VA("ALTER TRIGGER %{triggername}I ON %{identity}D RENAME TO %{newname}I",
											0);
				append_string_object(renameStmt, "triggername", node->subname);
				append_object_object(renameStmt, "identity",
									 new_objtree_for_qualname_id(RelationRelationId,
																 trigForm->tgrelid));
				relation_close(pg_trigger, AccessShareLock);
			}
			break;

		case OBJECT_POLICY:
			{
				HeapTuple	polTup;
				Form_pg_policy polForm;
				Relation	pg_policy;
				ScanKeyData	key;
				SysScanDesc	scan;

				pg_policy = relation_open(PolicyRelationId, AccessShareLock);
				ScanKeyInit(&key, ObjectIdAttributeNumber,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(address.objectId));
				scan = systable_beginscan(pg_policy, PolicyOidIndexId, true,
										  NULL, 1, &key);
				polTup = systable_getnext(scan);
				if (!HeapTupleIsValid(polTup))
					elog(ERROR, "cache lookup failed for policy %u", address.objectId);
				polForm = (Form_pg_policy) GETSTRUCT(polTup);

				renameStmt = new_objtree_VA("ALTER POLICY %{if_exists}s %{policyname}I on %{identity}D RENAME TO %{newname}I",
											0);
				append_string_object(renameStmt, "policyname", node->subname);
				append_object_object(renameStmt, "identity",
									 new_objtree_for_qualname_id(RelationRelationId,
																 polForm->polrelid));
				append_string_object(renameStmt, "if_exists",
									 node->missing_ok ? "IF EXISTS" : "");
				systable_endscan(scan);
				relation_close(pg_policy, AccessShareLock);
			}
			break;

		default:
			elog(ERROR, "unsupported object type %d", node->renameType);
	}

	append_string_object(renameStmt, "newname", node->newname);

	return renameStmt;
}

/*
 * deparse an ALTER ... SET SCHEMA command.
 */
static ObjTree *
deparse_AlterObjectSchemaStmt(ObjectAddress address, Node *parsetree,
							  ObjectAddress oldschema)
{
	AlterObjectSchemaStmt *node = (AlterObjectSchemaStmt *) parsetree;
	ObjTree	   *alterStmt;
	char	   *fmt;
	char	   *identity;
	char	   *newschema;
	char	   *oldschname;
	char	   *ident;

	newschema = node->newschema;

	fmt = psprintf("ALTER %s %%{identity}s SET SCHEMA %%{newschema}I",
				   stringify_objtype(node->objectType));
	alterStmt = new_objtree_VA(fmt, 0);
	append_string_object(alterStmt, "newschema", newschema);

	/*
	 * Since the command has already taken place from the point of view of
	 * catalogs, getObjectIdentity returns the object name with the already
	 * changed schema.  The output of our deparsing must return the original
	 * schema name however, so we chop the schema name off the identity string
	 * and then prepend the quoted schema name.
	 *
	 * XXX This is pretty clunky. Can we do better?
	 */
	identity = getObjectIdentity(&address);
	oldschname = get_namespace_name(oldschema.objectId);
	if (!oldschname)
		elog(ERROR, "cache lookup failed for schema with OID %u", oldschema.objectId);
	ident = psprintf("%s%s",
					 quote_identifier(oldschname),
					 identity + strlen(quote_identifier(newschema)));
	append_string_object(alterStmt, "identity", ident);

	return alterStmt;
}

static ObjTree *
deparse_AlterOwnerStmt(ObjectAddress address, Node *parsetree)
{
	AlterOwnerStmt *node = (AlterOwnerStmt *) parsetree;
	ObjTree	   *ownerStmt;
	char	   *fmt;

	fmt = psprintf("ALTER %s %%{identity}s OWNER TO %%{newowner}I",
				   stringify_objtype(node->objectType));
	ownerStmt = new_objtree_VA(fmt, 0);
	append_string_object(ownerStmt, "newowner",
						 get_rolespec_name(node->newowner));

	append_string_object(ownerStmt, "identity",
						 getObjectIdentity(&address));

	return ownerStmt;
}

static inline ObjElem *
deparse_Seq_Cache(ObjTree *parent, Form_pg_sequence seqdata)
{
	ObjTree	   *tmp;
	char	   *tmpstr;

	tmpstr = psprintf(INT64_FORMAT, seqdata->cache_value);
	tmp = new_objtree_VA("CACHE %{value}s",
						 2,
						 "clause", ObjTypeString, "cache",
						 "value", ObjTypeString, tmpstr);
	return new_object_object(tmp);
}

static inline ObjElem *
deparse_Seq_Cycle(ObjTree *parent, Form_pg_sequence seqdata)
{
	ObjTree	   *tmp;

	tmp = new_objtree_VA("%{no}s CYCLE",
						 2,
						 "clause", ObjTypeString, "cycle",
						 "no", ObjTypeString,
						 seqdata->is_cycled ? "" : "NO");
	return new_object_object(tmp);
}

static inline ObjElem *
deparse_Seq_IncrementBy(ObjTree *parent, Form_pg_sequence seqdata)
{
	ObjTree	   *tmp;
	char	   *tmpstr;

	tmpstr = psprintf(INT64_FORMAT, seqdata->increment_by);
	tmp = new_objtree_VA("INCREMENT BY %{value}s",
						 2,
						 "clause", ObjTypeString, "increment_by",
						 "value", ObjTypeString, tmpstr);
	return new_object_object(tmp);
}

static inline ObjElem *
deparse_Seq_Minvalue(ObjTree *parent, Form_pg_sequence seqdata)
{
	ObjTree	   *tmp;
	char	   *tmpstr;

	tmpstr = psprintf(INT64_FORMAT, seqdata->min_value);
	tmp = new_objtree_VA("MINVALUE %{value}s",
						 2,
						 "clause", ObjTypeString, "minvalue",
						 "value", ObjTypeString, tmpstr);
	return new_object_object(tmp);
}

static inline ObjElem *
deparse_Seq_Maxvalue(ObjTree *parent, Form_pg_sequence seqdata)
{
	ObjTree	   *tmp;
	char	   *tmpstr;

	tmpstr = psprintf(INT64_FORMAT, seqdata->max_value);
	tmp = new_objtree_VA("MAXVALUE %{value}s",
						 2,
						 "clause", ObjTypeString, "maxvalue",
						 "value", ObjTypeString, tmpstr);
	return new_object_object(tmp);
}

static inline ObjElem *
deparse_Seq_Startwith(ObjTree *parent, Form_pg_sequence seqdata)
{
	ObjTree	   *tmp;
	char	   *tmpstr;

	tmpstr = psprintf(INT64_FORMAT, seqdata->start_value);
	tmp = new_objtree_VA("START WITH %{value}s",
						 2,
						 "clause", ObjTypeString, "start",
						 "value", ObjTypeString, tmpstr);
	return new_object_object(tmp);
}

static inline ObjElem *
deparse_Seq_Restart(ObjTree *parent, Form_pg_sequence seqdata)
{
	ObjTree	   *tmp;
	char	   *tmpstr;

	tmpstr = psprintf(INT64_FORMAT, seqdata->last_value);
	tmp = new_objtree_VA("RESTART %{value}s",
						 2,
						 "clause", ObjTypeString, "restart",
						 "value", ObjTypeString, tmpstr);
	return new_object_object(tmp);
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

		tmp = new_objtree_for_qualname_id(RelationRelationId, ownerId);
		append_string_object(tmp, "attrname", colname);
		ownedby = new_objtree_VA("OWNED BY %{owner}D",
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
		ownedby = new_objtree_VA("OWNED BY %{owner}D",
								 3,
								 "clause", ObjTypeString, "owned",
								 "owner", ObjTypeNull,
								 "present", ObjTypeBool, false);
	return new_object_object(ownedby);
}

/*
 * deparse_CreateSeqStmt
 *		deparse a CreateSeqStmt
 *
 * Given a sequence OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 */
static ObjTree *
deparse_CreateSeqStmt(Oid objectId, Node *parsetree)
{
	ObjTree    *createSeq;
	ObjTree    *tmp;
	Relation	relation = relation_open(objectId, AccessShareLock);
	Form_pg_sequence seqdata;
	List	   *elems = NIL;

	seqdata = get_sequence_values(objectId);

	createSeq =
		new_objtree_VA("CREATE %{persistence}s SEQUENCE %{identity}D "
					   "%{definition: }s",
					   1,
					   "persistence", ObjTypeString,
					   get_persistence_str(relation->rd_rel->relpersistence));

	tmp = new_objtree_for_qualname(relation->rd_rel->relnamespace,
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

	relation_close(relation, AccessShareLock);

	return createSeq;
}

/*
 * deparse_AlterSeqStmt
 *		deparse an AlterSeqStmt
 *
 * Given a sequence OID and a parsetree that modified it, return an ObjTree
 * representing the alter command.
 */
static ObjTree *
deparse_AlterSeqStmt(Oid objectId, Node *parsetree)
{
	ObjTree	   *alterSeq;
	ObjTree	   *tmp;
	Relation	relation = relation_open(objectId, AccessShareLock);
	Form_pg_sequence seqdata;
	List	   *elems = NIL;
	ListCell   *cell;

	seqdata = get_sequence_values(objectId);

	alterSeq =
		new_objtree_VA("ALTER SEQUENCE %{identity}D %{definition: }s", 0);
	tmp = new_objtree_for_qualname(relation->rd_rel->relnamespace,
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
		else if (strcmp(elem->defname, "increment") == 0)
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

	relation_close(relation, AccessShareLock);

	return alterSeq;
}

/*
 * deparse_IndexStmt
 *		deparse an IndexStmt
 *
 * Given an index OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 *
 * If the index corresponds to a constraint, NULL is returned.
 */
static ObjTree *
deparse_IndexStmt(Oid objectId, Node *parsetree)
{
	IndexStmt  *node = (IndexStmt *) parsetree;
	ObjTree    *indexStmt;
	ObjTree    *tmp;
	Relation	idxrel;
	Relation	heaprel;
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
		new_objtree_VA("CREATE %{unique}s INDEX %{concurrently}s %{if_not_exists}s %{name}I "
					   "ON %{table}D USING %{index_am}s (%{definition}s) "
					   "%{with}s %{tablespace}s %{where_clause}s",
					   6,
					   "unique", ObjTypeString, node->unique ? "UNIQUE" : "",
					   "concurrently", ObjTypeString,
					   node->concurrent ? "CONCURRENTLY" : "",
					   "if_not_exists", ObjTypeString, node->if_not_exists ? "IF NOT EXISTS" : "",
					   "name", ObjTypeString, RelationGetRelationName(idxrel),
					   "definition", ObjTypeString, definition,
					   "index_am", ObjTypeString, index_am);

	tmp = new_objtree_for_qualname(heaprel->rd_rel->relnamespace,
								   RelationGetRelationName(heaprel));
	append_object_object(indexStmt, "table", tmp);

	/* reloptions */
	tmp = new_objtree_VA("WITH (%{opts}s)", 0);
	if (reloptions)
		append_string_object(tmp, "opts", reloptions);
	else
		append_bool_object(tmp, "present", false);
	append_object_object(indexStmt, "with", tmp);

	/* tablespace */
	tmp = new_objtree_VA("TABLESPACE %{tablespace}s", 0);
	if (tablespace)
		append_string_object(tmp, "tablespace", tablespace);
	else
		append_bool_object(tmp, "present", false);
	append_object_object(indexStmt, "tablespace", tmp);

	/* WHERE clause */
	tmp = new_objtree_VA("WHERE %{where}s", 0);
	if (whereClause)
		append_string_object(tmp, "where", whereClause);
	else
		append_bool_object(tmp, "present", false);
	append_object_object(indexStmt, "where_clause", tmp);

	heap_close(idxrel, AccessShareLock);
	heap_close(heaprel, AccessShareLock);

	return indexStmt;
}

static ObjTree *
deparse_RuleStmt(Oid objectId, Node *parsetree)
{
	RuleStmt *node = (RuleStmt *) parsetree;
	ObjTree	   *ruleStmt;
	ObjTree	   *tmp;
	Relation	pg_rewrite;
	Form_pg_rewrite rewrForm;
	HeapTuple	rewrTup;
	SysScanDesc	scan;
	ScanKeyData	key;
	Datum		ev_qual;
	Datum		ev_actions;
	bool		isnull;
	char	   *qual;
	List	   *actions;
	List	   *list;
	ListCell   *cell;

	pg_rewrite = heap_open(RewriteRelationId, AccessShareLock);
	ScanKeyInit(&key,
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(objectId));

	scan = systable_beginscan(pg_rewrite, RewriteOidIndexId, true,
							  NULL, 1, &key);
	rewrTup = systable_getnext(scan);
	if (!HeapTupleIsValid(rewrTup))
		elog(ERROR, "cache lookup failed for rewrite rule with oid %u",
			 objectId);

	rewrForm = (Form_pg_rewrite) GETSTRUCT(rewrTup);

	ruleStmt =
		new_objtree_VA("CREATE %{or_replace}s RULE %{identity}I "
					   "AS ON %{event}s TO %{table}D %{where_clause}s "
					   "DO %{instead}s (%{actions:; }s)", 2,
					   "identity", ObjTypeString, node->rulename,
					   "or_replace", ObjTypeString,
					   node->replace ? "OR REPLACE" : "");
	append_string_object(ruleStmt, "event",
						 node->event == CMD_SELECT ? "SELECT" :
						 node->event == CMD_UPDATE ? "UPDATE" :
						 node->event == CMD_DELETE ? "DELETE" :
						 node->event == CMD_INSERT ? "INSERT" : "XXX");
	append_object_object(ruleStmt, "table",
						 new_objtree_for_qualname_id(RelationRelationId,
													 rewrForm->ev_class));

	append_string_object(ruleStmt, "instead",
						 node->instead ? "INSTEAD" : "ALSO");

	ev_qual = heap_getattr(rewrTup, Anum_pg_rewrite_ev_qual,
						   RelationGetDescr(pg_rewrite), &isnull);
	ev_actions = heap_getattr(rewrTup, Anum_pg_rewrite_ev_action,
							  RelationGetDescr(pg_rewrite), &isnull);

	pg_get_ruledef_details(ev_qual, ev_actions, &qual, &actions);

	tmp = new_objtree_VA("WHERE %{clause}s", 0);

	if (qual)
		append_string_object(tmp, "clause", qual);
	else
	{
		append_null_object(tmp, "clause");
		append_bool_object(tmp, "present", false);
	}

	append_object_object(ruleStmt, "where_clause", tmp);

	list = NIL;
	foreach(cell, actions)
	{
		char *action = lfirst(cell);

		list = lappend(list, new_string_object(action));
	}
	append_array_object(ruleStmt, "actions", list);

	systable_endscan(scan);
	heap_close(pg_rewrite, AccessShareLock);

	return ruleStmt;
}

/*
 * deparse_CreateSchemaStmt
 *		deparse a CreateSchemaStmt
 *
 * Given a schema OID and the parsetree that created it, return an ObjTree
 * representing the creation command.
 *
 * Note we don't output the schema elements given in the creation command.
 * They must be output separately.	 (In the current implementation,
 * CreateSchemaCommand passes them back to ProcessUtility, which will lead to
 * this file if appropriate.)
 */
static ObjTree *
deparse_CreateSchemaStmt(Oid objectId, Node *parsetree)
{
	CreateSchemaStmt *node = (CreateSchemaStmt *) parsetree;
	ObjTree    *createSchema;
	ObjTree    *auth;

	createSchema =
		new_objtree_VA("CREATE SCHEMA %{if_not_exists}s %{name}I %{authorization}s",
					   2,
					   "name", ObjTypeString, node->schemaname,
					   "if_not_exists", ObjTypeString,
					   node->if_not_exists ? "IF NOT EXISTS" : "");

	auth = new_objtree_VA("AUTHORIZATION %{authorization_role}I", 0);
	if (node->authrole)
		append_string_object(auth, "authorization_role",
							 get_rolespec_name(node->authrole));
	else
	{
		append_null_object(auth, "authorization_role");
		append_bool_object(auth, "present", false);
	}
	append_object_object(createSchema, "authorization", auth);

	return createSchema;
}

static ObjTree *
deparse_AlterEnumStmt(Oid objectId, Node *parsetree)
{
	AlterEnumStmt *node = (AlterEnumStmt *) parsetree;
	ObjTree	   *alterEnum;
	ObjTree	   *tmp;

	alterEnum =
		new_objtree_VA("ALTER TYPE %{identity}D ADD VALUE %{if_not_exists}s %{value}L %{position}s",
					   0);

	append_string_object(alterEnum, "if_not_exists",
						 node->skipIfExists ? "IF NOT EXISTS" : "");
	append_object_object(alterEnum, "identity",
						 new_objtree_for_qualname_id(TypeRelationId,
													 objectId));
	append_string_object(alterEnum, "value", node->newVal);
	tmp = new_objtree_VA("%{after_or_before}s %{neighbour}L", 0);
	if (node->newValNeighbor)
	{
		append_string_object(tmp, "after_or_before",
							 node->newValIsAfter ? "AFTER" : "BEFORE");
		append_string_object(tmp, "neighbour", node->newValNeighbor);
	}
	else
	{
		append_bool_object(tmp, "present", false);
	}
	append_object_object(alterEnum, "position", tmp);

	return alterEnum;
}

/*
 * Handle deparsing of simple commands.
 *
 * This function contains a large switch that mirrors that in
 * ProcessUtilitySlow.  All cases covered there should also be covered here.
 */
static ObjTree *
deparse_simple_command(StashedCommand *cmd)
{
	Oid			objectId;
	Node	   *parsetree;
	ObjTree	   *command;

	Assert(cmd->type == SCT_Simple);

	parsetree = cmd->parsetree;
	objectId = cmd->d.simple.address.objectId;

	/* This switch needs to handle everything that ProcessUtilitySlow does */
	switch (nodeTag(parsetree))
	{
		case T_CreateSchemaStmt:
			command = deparse_CreateSchemaStmt(objectId, parsetree);
			break;

		case T_CreateStmt:
			command = deparse_CreateStmt(objectId, parsetree);
			break;

		case T_CreateForeignTableStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_AlterTableStmt:
		case T_AlterTableMoveAllStmt:
			/* handled elsewhere */
			elog(ERROR, "unexpected command type %s", CreateCommandTag(parsetree));
			break;

		case T_AlterDomainStmt:
			command = deparse_AlterDomainStmt(objectId, parsetree,
											  cmd->d.simple.secondaryObject);
			break;

			/* other local objects */
		case T_DefineStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_IndexStmt:
			command = deparse_IndexStmt(objectId, parsetree);
			break;

		case T_CreateExtensionStmt:
			command = deparse_CreateExtensionStmt(objectId, parsetree);
			break;

		case T_AlterExtensionStmt:
			command = deparse_AlterExtensionStmt(objectId, parsetree);
			break;

		case T_AlterExtensionContentsStmt:
			command = deparse_AlterExtensionContentsStmt(objectId, parsetree,
														 cmd->d.simple.secondaryObject);
			break;

		case T_CreateFdwStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_AlterFdwStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateForeignServerStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_AlterForeignServerStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateUserMappingStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_AlterUserMappingStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_DropUserMappingStmt:
			/* goes through performDeletion; no action needed here */
			command = NULL;
			break;

		case T_ImportForeignSchemaStmt:
			/* generated commands are stashed individually */
			elog(ERROR, "unexpected command %s", CreateCommandTag(parsetree));
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

		case T_AlterEnumStmt:
			command = deparse_AlterEnumStmt(objectId, parsetree);
			break;

		case T_ViewStmt:		/* CREATE VIEW */
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateFunctionStmt:
			command = deparse_CreateFunction(objectId, parsetree);
			break;

		case T_AlterFunctionStmt:
			command = deparse_AlterFunction(objectId, parsetree);
			break;

		case T_RuleStmt:
			command = deparse_RuleStmt(objectId, parsetree);
			break;

		case T_CreateSeqStmt:
			command = deparse_CreateSeqStmt(objectId, parsetree);
			break;

		case T_AlterSeqStmt:
			command = deparse_AlterSeqStmt(objectId, parsetree);
			break;

		case T_CreateTableAsStmt:
			/* XXX handle at least the CREATE MATVIEW case? */
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_RefreshMatViewStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateTrigStmt:
			command = deparse_CreateTrigStmt(objectId, parsetree);
			break;

		case T_CreatePLangStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateDomainStmt:
			command = deparse_CreateDomain(objectId, parsetree);
			break;

		case T_CreateConversionStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateCastStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateOpClassStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateOpFamilyStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_AlterOpFamilyStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_AlterTSDictionaryStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_AlterTSConfigurationStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_DropStmt:
			/* goes through performDeletion; no action needed here */
			command = NULL;
			break;

		case T_RenameStmt:
			command = deparse_RenameStmt(cmd->d.simple.address, parsetree);
			break;

		case T_AlterObjectSchemaStmt:
			command = deparse_AlterObjectSchemaStmt(cmd->d.simple.address,
													parsetree,
													cmd->d.simple.secondaryObject);
			break;

		case T_AlterOwnerStmt:
			command = deparse_AlterOwnerStmt(cmd->d.simple.address, parsetree);
			break;

		case T_CommentStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_GrantStmt:
			/* handled elsewhere */
			elog(ERROR, "unexpected command type %s", CreateCommandTag(parsetree));
			break;

		case T_DropOwnedStmt:
			/* goes through performDeletion; no action needed here */
			command = NULL;
			break;

		case T_AlterDefaultPrivilegesStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreatePolicyStmt:	/* CREATE POLICY */
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_AlterPolicyStmt:		/* ALTER POLICY */
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_SecLabelStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		default:
			command = NULL;
			elog(LOG, "unrecognized node type: %d",
				 (int) nodeTag(parsetree));
	}

	return command;
}

/*
 * Given a StashedCommand, return a JSON representation of the command.
 *
 * The command is expanded fully, so that there are no ambiguities even in the
 * face of search_path changes.
 */
char *
deparse_utility_command(StashedCommand *cmd)
{
	OverrideSearchPath *overridePath;
	MemoryContext	oldcxt;
	MemoryContext	tmpcxt;
	ObjTree		   *tree;
	char		   *command;
	StringInfoData  str;

	/*
	 * Allocate everything done by the deparsing routines into a temp context,
	 * to avoid having to sprinkle them with memory handling code; but allocate
	 * the output StringInfo before switching.
	 */
	initStringInfo(&str);
	tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
								   "deparse ctx",
								   ALLOCSET_DEFAULT_MINSIZE,
								   ALLOCSET_DEFAULT_INITSIZE,
								   ALLOCSET_DEFAULT_MAXSIZE);
	oldcxt = MemoryContextSwitchTo(tmpcxt);

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
	overridePath->addCatalog = false;
	overridePath->addTemp = true;
	PushOverrideSearchPath(overridePath);

	switch (cmd->type)
	{
		case SCT_Simple:
			tree = deparse_simple_command(cmd);
			break;
		default:
			elog(ERROR, "unexpected deparse node type %d", cmd->type);
	}

	PopOverrideSearchPath();

	if (tree)
	{
		Jsonb *jsonb;

		jsonb = objtree_to_jsonb(tree);
		command = JsonbToCString(&str, &jsonb->root, 128);
	}
	else
		command = NULL;

	/*
	 * Clean up.  Note that since we created the StringInfo in the caller's
	 * context, the output string is not deleted here.
	 */
	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(tmpcxt);

	return command;
}
