/*-------------------------------------------------------------------------
 *
 * ddl_json.c
 *	  JSON code related to DDL command deparsing
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/ddl_json.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"


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

typedef enum
{
	tv_absent,
	tv_true,
	tv_false
} trivalue;

static void expand_one_jsonb_element(StringInfo out, char *param,
						 JsonbValue *jsonval, convSpecifier specifier,
						 const char *fmt);
static void expand_jsonb_array(StringInfo out, char *param,
				   JsonbValue *jsonarr, char *arraysep,
				   convSpecifier specifier, const char *fmt);
static void fmtstr_error_callback(void *arg);

static trivalue
find_bool_in_jsonbcontainer(JsonbContainer *container, char *keyname)
{
	JsonbValue	key;
	JsonbValue *value;
	bool		result;

	key.type = jbvString;
	key.val.string.val = keyname;
	key.val.string.len = strlen(keyname);
	value = findJsonbValueFromContainer(container,
										JB_FOBJECT, &key);
	if (value == NULL)
		return tv_absent;
	if (value->type != jbvBool)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("element \"%s\" is not of type boolean",
						keyname)));
	result = value->val.boolean ? tv_true : tv_false;
	pfree(value);

	return result;
}

/*
 * Given a JsonbContainer, find the JsonbValue with the given key name in it.
 * If it's of a type other than jbvString, an error is raised.  If it doesn't
 * exist, an error is raised if missing_ok; otherwise return NULL.
 *
 * If it exists and is a string, a freshly palloc'ed copy is returned.
 *
 * If *length is not NULL, it is set to the length of the string.
 */
static char *
find_string_in_jsonbcontainer(JsonbContainer *container, char *keyname,
							  bool missing_ok, int *length)
{
	JsonbValue	key;
	JsonbValue *value;
	char	   *str;

	/* XXX verify that this is an object, not an array */

	key.type = jbvString;
	key.val.string.val = keyname;
	key.val.string.len = strlen(keyname);
	value = findJsonbValueFromContainer(container,
										JB_FOBJECT, &key);
	if (value == NULL)
	{
		if (missing_ok)
			return NULL;
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("missing element \"%s\" in json object", keyname)));
	}

	str = pnstrdup(value->val.string.val, value->val.string.len);
	if (length)
		*length = value->val.string.len;
	pfree(value);
	return str;
}

#define ADVANCE_PARSE_POINTER(ptr,end_ptr) \
	do { \
		if (++(ptr) >= (end_ptr)) \
			ereport(ERROR, \
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
					 errmsg("unterminated format specifier"))); \
	} while (0)

/*
 * Recursive helper for pg_event_trigger_expand_command
 *
 * Find the "fmt" element in the given container, and expand it into the
 * provided StringInfo.
 */
static void
expand_fmt_recursive(JsonbContainer *container, StringInfo out)
{
	JsonbValue	key;
	JsonbValue *value;
	const char *cp;
	const char *start_ptr;
	const char *end_ptr;
	int			len;

	start_ptr = find_string_in_jsonbcontainer(container, "fmt", false, &len);
	end_ptr = start_ptr + len;

	for (cp = start_ptr; cp < end_ptr; cp++)
	{
		convSpecifier specifier;
		bool		is_array;
		char	   *param = NULL;
		char	   *arraysep = NULL;

		if (*cp != '%')
		{
			appendStringInfoCharMacro(out, *cp);
			continue;
		}

		is_array = false;

		ADVANCE_PARSE_POINTER(cp, end_ptr);

		/* Easy case: %% outputs a single % */
		if (*cp == '%')
		{
			appendStringInfoCharMacro(out, *cp);
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
			case 'L':
				specifier = SpecStringLiteral;
				break;
			case 'T':
				specifier = SpecTypename;
				break;
			case 'O':
				specifier = SpecOperatorname;
				break;
			case 'n':
				specifier = SpecNumber;
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid conversion specifier \"%c\"", *cp)));
		}

		/*
		 * Obtain the element to be expanded.
		 */
		key.type = jbvString;
		key.val.string.val = param;
		key.val.string.len = strlen(param);

		value = findJsonbValueFromContainer(container, JB_FOBJECT, &key);

		/* Validate that we got an array if the format string specified one. */

		/* And finally print out the data */
		if (is_array)
			expand_jsonb_array(out, param, value, arraysep, specifier, start_ptr);
		else
			expand_one_jsonb_element(out, param, value, specifier, start_ptr);
	}
}

/*
 * Expand a json value as an identifier.  The value must be of type string.
 */
static void
expand_jsonval_identifier(StringInfo buf, JsonbValue *jsonval)
{
	char	   *str;

	Assert(jsonval->type == jbvString);

	str = pnstrdup(jsonval->val.string.val,
				   jsonval->val.string.len);
	appendStringInfoString(buf, quote_identifier(str));
	pfree(str);
}

/*
 * Expand a json value as a dot-separated-name.  The value must be of type
 * object and must contain elements "schemaname" (optional), "objname"
 * (mandatory), "attrname" (optional).  Double quotes are added to each element
 * as necessary, and dot separators where needed.
 *
 * One day we might need a "catalog" element as well, but no current use case
 * needs that.
 */
static void
expand_jsonval_dottedname(StringInfo buf, JsonbValue *jsonval)
{
	char	   *str;

	str = find_string_in_jsonbcontainer(jsonval->val.binary.data,
										"schemaname", true, NULL);
	if (str)
	{
		appendStringInfo(buf, "%s.", quote_identifier(str));
		pfree(str);
	}

	str = find_string_in_jsonbcontainer(jsonval->val.binary.data,
										"objname", false, NULL);
	appendStringInfo(buf, "%s", quote_identifier(str));
	pfree(str);

	str = find_string_in_jsonbcontainer(jsonval->val.binary.data,
										"attrname", true, NULL);
	if (str)
	{
		appendStringInfo(buf, ".%s", quote_identifier(str));
		pfree(str);
	}
}

/*
 * expand a json value as a type name.
 */
static void
expand_jsonval_typename(StringInfo buf, JsonbValue *jsonval)
{
	char	   *schema = NULL;
	char	   *typename;
	char	   *typmodstr;
	trivalue	is_array;
	char	   *array_decor;

	/*
	 * We omit schema-qualifying the output name if the schema element is
	 * either the empty string or NULL; the difference between those two cases
	 * is that in the latter we quote the type name, in the former we don't.
	 * This allows for types with special typmod needs, such as interval and
	 * timestamp (see format_type_detailed), while at the same time allowing
	 * for the schema name to be omitted from type names that require quotes
	 * but are to be obtained from a user schema.
	 */

	schema = find_string_in_jsonbcontainer(jsonval->val.binary.data,
										   "schemaname", true, NULL);
	typename = find_string_in_jsonbcontainer(jsonval->val.binary.data,
											 "typename", false, NULL);
	typmodstr = find_string_in_jsonbcontainer(jsonval->val.binary.data,
											  "typmod", true, NULL);
	is_array = find_bool_in_jsonbcontainer(jsonval->val.binary.data,
										   "is_array");
	switch (is_array)
	{
		default:
		case tv_absent:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("missing is_array element")));
			break;
		case tv_true:
			array_decor = "[]";
			break;
		case tv_false:
			array_decor = "";
			break;
	}

	if (schema == NULL)
		appendStringInfo(buf, "%s%s%s",
						 quote_identifier(typename),
						 typmodstr ? typmodstr : "",
						 array_decor);
	else if (schema[0] == '\0')
		appendStringInfo(buf, "%s%s%s",
						 typename,
						 typmodstr ? typmodstr : "",
						 array_decor);
	else
		appendStringInfo(buf, "%s.%s%s%s",
						 quote_identifier(schema),
						 quote_identifier(typename),
						 typmodstr ? typmodstr : "",
						 array_decor);
}

/*
 * Expand a json value as an operator name
 */
static void
expand_jsonval_operator(StringInfo buf, JsonbValue *jsonval)
{
	char	   *str;

	str = find_string_in_jsonbcontainer(jsonval->val.binary.data,
										"schemaname", true, NULL);
	/* schema might be NULL or empty */
	if (str != NULL && str[0] != '\0')
		appendStringInfo(buf, "%s.", quote_identifier(str));

	str = find_string_in_jsonbcontainer(jsonval->val.binary.data,
										"objname", false, NULL);
	appendStringInfoString(buf, str);
}

/*
 * Expand a json value as a string.  The value must be of type string or of
 * type object.  In the latter case it must contain a "fmt" element which will
 * be recursively expanded; also, if the object contains an element "present"
 * and it is set to false, the expansion is the empty string.
 */
static void
expand_jsonval_string(StringInfo buf, JsonbValue *jsonval)
{
	if (jsonval->type == jbvString)
	{
		appendBinaryStringInfo(buf, jsonval->val.string.val,
							   jsonval->val.string.len);
	}
	else if (jsonval->type == jbvBinary)
	{
		trivalue	present;

		present = find_bool_in_jsonbcontainer(jsonval->val.binary.data,
											  "present");
		/*
		 * If "present" is set to false, this element expands to empty;
		 * otherwise (either true or absent), fall through to expand "fmt".
		 */
		if (present == tv_false)
			return;

		expand_fmt_recursive(jsonval->val.binary.data, buf);
	}
}

/*
 * Expand a json value as a string literal
 */
static void
expand_jsonval_strlit(StringInfo buf, JsonbValue *jsonval)
{
	char   *str;
	StringInfoData dqdelim;
	static const char dqsuffixes[] = "_XYZZYX_";
	int         dqnextchar = 0;

	str = pnstrdup(jsonval->val.string.val, jsonval->val.string.len);

	/* easy case: if there are no ' and no \, just use a single quote */
	if (strchr(str, '\'') == NULL &&
		strchr(str, '\\') == NULL)
	{
		appendStringInfo(buf, "'%s'", str);
		pfree(str);
		return;
	}

	/* Otherwise need to find a useful dollar-quote delimiter */
	initStringInfo(&dqdelim);
	appendStringInfoString(&dqdelim, "$");
	while (strstr(str, dqdelim.data) != NULL)
	{
		appendStringInfoChar(&dqdelim, dqsuffixes[dqnextchar++]);
		dqnextchar %= sizeof(dqsuffixes) - 1;
	}
	/* add trailing $ */
	appendStringInfoChar(&dqdelim, '$');

	/* And finally produce the quoted literal into the output StringInfo */
	appendStringInfo(buf, "%s%s%s", dqdelim.data, str, dqdelim.data);
	pfree(dqdelim.data);
	pfree(str);
}

/*
 * Expand a json value as an integer quantity
 */
static void
expand_jsonval_number(StringInfo buf, JsonbValue *jsonval)
{
	char *strdatum;

	strdatum = DatumGetCString(DirectFunctionCall1(numeric_out,
												   NumericGetDatum(jsonval->val.numeric)));
	appendStringInfoString(buf, strdatum);
}

/*
 * Expand one json element into the output StringInfo according to the
 * conversion specifier.  The element type is validated, and an error is raised
 * if it doesn't match what we expect for the conversion specifier.
 */
static void
expand_one_jsonb_element(StringInfo out, char *param, JsonbValue *jsonval,
						 convSpecifier specifier, const char *fmt)
{
	ErrorContextCallback sqlerrcontext;

	/* If we were given a format string, setup an ereport() context callback */
	if (fmt)
	{
		sqlerrcontext.callback = fmtstr_error_callback;
		sqlerrcontext.arg = (void *) fmt;
		sqlerrcontext.previous = error_context_stack;
		error_context_stack = &sqlerrcontext;
	}

	if (!jsonval)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("element \"%s\" not found", param)));

	switch (specifier)
	{
		case SpecIdentifier:
			if (jsonval->type != jbvString)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("expected JSON string for %%I element \"%s\", got %d",
								param, jsonval->type)));
			expand_jsonval_identifier(out, jsonval);
			break;

		case SpecDottedName:
			if (jsonval->type != jbvBinary)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("expected JSON object for %%D element \"%s\", got %d",
								param, jsonval->type)));
			expand_jsonval_dottedname(out, jsonval);
			break;

		case SpecString:
			if (jsonval->type != jbvString &&
				jsonval->type != jbvBinary)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("expected JSON string or object for %%s element \"%s\", got %d",
								param, jsonval->type)));
			expand_jsonval_string(out, jsonval);
			break;

		case SpecStringLiteral:
			if (jsonval->type != jbvString)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("expected JSON string for %%L element \"%s\", got %d",
								param, jsonval->type)));
			expand_jsonval_strlit(out, jsonval);
			break;

		case SpecTypename:
			if (jsonval->type != jbvBinary)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("expected JSON object for %%T element \"%s\", got %d",
								param, jsonval->type)));
			expand_jsonval_typename(out, jsonval);
			break;

		case SpecOperatorname:
			if (jsonval->type != jbvBinary)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("expected JSON object for %%O element \"%s\", got %d",
								param, jsonval->type)));
			expand_jsonval_operator(out, jsonval);
			break;

		case SpecNumber:
			if (jsonval->type != jbvNumeric)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("expected JSON numeric for %%n element \"%s\", got %d",
								param, jsonval->type)));
			expand_jsonval_number(out, jsonval);
			break;
	}

	if (fmt)
		error_context_stack = sqlerrcontext.previous;
}

/*
 * Iterate on the elements of a JSON array, expanding each one into the output
 * StringInfo per the given conversion specifier, separated by the given
 * separator.
 */
static void
expand_jsonb_array(StringInfo out, char *param,
				   JsonbValue *jsonarr, char *arraysep, convSpecifier specifier,
				   const char *fmt)
{
	ErrorContextCallback sqlerrcontext;
	JsonbContainer *container;
	JsonbIterator  *it;
	JsonbValue	v;
	int			type;
	bool		first = true;

	/* If we were given a format string, setup an ereport() context callback */
	if (fmt)
	{
		sqlerrcontext.callback = fmtstr_error_callback;
		sqlerrcontext.arg = (void *) fmt;
		sqlerrcontext.previous = error_context_stack;
		error_context_stack = &sqlerrcontext;
	}

	if (jsonarr->type != jbvBinary)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("element \"%s\" is not a JSON array", param)));

	container = jsonarr->val.binary.data;
	if ((container->header & JB_FARRAY) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("element \"%s\" is not a JSON array", param)));

	it = JsonbIteratorInit(container);
	while ((type = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
	{
		switch (type)
		{
			case WJB_ELEM:
				if (!first)
					appendStringInfoString(out, arraysep);
				first = false;
				expand_one_jsonb_element(out, param, &v, specifier, NULL);
				break;
		}
	}

	if (fmt)
		error_context_stack = sqlerrcontext.previous;
}

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
 * n		expand as a simple number (no quoting)
 *
 * The element name may have an optional separator specification preceded
 * by a colon.	Its presence indicates that the element is expected to be
 * an array; the specified separator is used to join the array elements.
 *------
 */
Datum
pg_event_trigger_expand_command(PG_FUNCTION_ARGS)
{
	text	   *json = PG_GETARG_TEXT_P(0);
	Datum		d;
	Jsonb	   *jsonb;
	StringInfoData out;

	initStringInfo(&out);
	d = DirectFunctionCall1(jsonb_in,
							PointerGetDatum(TextDatumGetCString(json)));
	jsonb = (Jsonb *) DatumGetPointer(d);

	expand_fmt_recursive(&jsonb->root, &out);

	PG_RETURN_TEXT_P(CStringGetTextDatum(out.data));
}

/*
 * Error context callback for JSON format string expansion.
 *
 * Possible improvement: indicate which element we're expanding, if applicable
 */
static void
fmtstr_error_callback(void *arg)
{
	errcontext("while expanding format string \"%s\"", (char *) arg);

}
