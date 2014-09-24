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
#include "catalog/pg_proc.h"
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
#include "tcop/utility.h"
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
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
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
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

			/* other local objects */
		case T_DefineStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_IndexStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateExtensionStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_AlterExtensionStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_AlterExtensionContentsStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
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
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateEnumStmt:	/* CREATE TYPE AS ENUM */
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateRangeStmt:	/* CREATE TYPE AS RANGE */
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_AlterEnumStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_ViewStmt:		/* CREATE VIEW */
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateFunctionStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_AlterFunctionStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_RuleStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateSeqStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_AlterSeqStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateTableAsStmt:
			/* XXX handle at least the CREATE MATVIEW case? */
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_RefreshMatViewStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateTrigStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreatePLangStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_CreateDomainStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
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
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_AlterObjectSchemaStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
			break;

		case T_AlterOwnerStmt:
			elog(ERROR, "unimplemented deparse of %s", CreateCommandTag(parsetree));
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
 * Given a utility command parsetree and the OID of the corresponding object,
 * return a JSON representation of the command.
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
