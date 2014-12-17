---
--- CREATE_TYPE
---

CREATE FUNCTION text_w_default_in(cstring)
   RETURNS text_w_default
   AS 'textin'
   LANGUAGE internal STABLE STRICT;

CREATE FUNCTION text_w_default_out(text_w_default)
   RETURNS cstring
   AS 'textout'
   LANGUAGE internal STABLE STRICT ;

CREATE TYPE text_w_default (
   internallength = variable,
   input = text_w_default_in,
   output = text_w_default_out,
   alignment = int4,
   default = 'foo',
   collatable = TRUE
);

CREATE TYPE employee_type AS (name TEXT, salary NUMERIC);

