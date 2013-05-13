%{
/*-------------------------------------------------------------------------
 *
 * specparse.y
 *	  bison grammar for the isolation test file format
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "isolationtester.h"


TestSpec		parseresult;			/* result of parsing is left here */

%}

%expect 0
%name-prefix="spec_yy"

%union
{
	char	   *str;
	Connection *connection;
	Session	   *session;
	Step	   *step;
	Permutation *permutation;
	struct
	{
		void  **elements;
		int		nelements;
	}			ptr_list;
}

%type <ptr_list> setup_list conninfo_list
%type <str> opt_setup opt_teardown opt_connection
%type <str> setup connection
%type <ptr_list> step_list session_list permutation_list opt_permutation_list
%type <ptr_list> string_list
%type <session> session
%type <step> step
%type <permutation> permutation
%type <connection> conninfo

%token <str> sqlblock string
%token CONNINFO PERMUTATION SESSION CONNECTION SETUP STEP TEARDOWN TEST

%%

TestSpec:
			conninfo_list
			setup_list
			opt_teardown
			session_list
			opt_permutation_list
			{
				parseresult.conninfos = (char **) $1.elements;
				parseresult.nconninfos = $1.nelements;
				parseresult.setupsqls = (char **) $2.elements;
				parseresult.nsetupsqls = $2.nelements;
				parseresult.teardownsql = $3;
				parseresult.sessions = (Session **) $4.elements;
				parseresult.nsessions = $4.nelements;
				parseresult.permutations = (Permutation **) $5.elements;
				parseresult.npermutations = $5.nelements;
			}
		;

conninfo_list:
			/* EMPTY */
			{
				$$.elements = NULL;
				$$.nelements = 0;
			}
			| conninfo_list conninfo
			{
				$$.elements = realloc($1.elements,
									  ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
		;

conninfo:
			CONNINFO string string
			{
				$$ = malloc(sizeof(Connection));
				$$->name = $2;
				$$->conninfo = $3;
			}
		;

setup_list:
			/* EMPTY */
			{
				$$.elements = NULL;
				$$.nelements = 0;
			}
			| setup_list setup
			{
				$$.elements = realloc($1.elements,
									  ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
		;

opt_setup:
			/* EMPTY */			{ $$ = NULL; }
			| setup				{ $$ = $1; }
		;

setup:
			SETUP sqlblock		{ $$ = $2; }
		;

opt_teardown:
			/* EMPTY */			{ $$ = NULL; }
			| TEARDOWN sqlblock	{ $$ = $2; }
		;

session_list:
			session_list session
			{
				$$.elements = realloc($1.elements,
									  ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
			| session
			{
				$$.nelements = 1;
				$$.elements = malloc(sizeof(void *));
				$$.elements[0] = $1;
			}
		;

opt_connection:
			/* EMPTY */         { $$ = NULL; }
			| connection        { $$ = $1; }
		;

connection:
			CONNECTION string   { $$ = $2; }
		;

session:
			SESSION string opt_connection opt_setup step_list opt_teardown
			{
				$$ = malloc(sizeof(Session));
				$$->name = $2;
				$$->connection = $3;
				$$->setupsql = $4;
				$$->steps = (Step **) $5.elements;
				$$->nsteps = $5.nelements;
				$$->teardownsql = $6;
			}
		;

step_list:
			step_list step
			{
				$$.elements = realloc($1.elements,
									  ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
			| step
			{
				$$.nelements = 1;
				$$.elements = malloc(sizeof(void *));
				$$.elements[0] = $1;
			}
		;


step:
			STEP string sqlblock
			{
				$$ = malloc(sizeof(Step));
				$$->name = $2;
				$$->sql = $3;
				$$->errormsg = NULL;
			}
		;


opt_permutation_list:
			permutation_list
			{
				$$ = $1;
			}
			| /* EMPTY */
			{
				$$.elements = NULL;
				$$.nelements = 0;
			}

permutation_list:
			permutation_list permutation
			{
				$$.elements = realloc($1.elements,
									  ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
			| permutation
			{
				$$.nelements = 1;
				$$.elements = malloc(sizeof(void *));
				$$.elements[0] = $1;
			}
		;


permutation:
			PERMUTATION string_list
			{
				$$ = malloc(sizeof(Permutation));
				$$->stepnames = (char **) $2.elements;
				$$->nsteps = $2.nelements;
			}
		;

string_list:
			string_list string
			{
				$$.elements = realloc($1.elements,
									  ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
			| string
			{
				$$.nelements = 1;
				$$.elements = malloc(sizeof(void *));
				$$.elements[0] = $1;
			}
		;

%%

#include "specscanner.c"
