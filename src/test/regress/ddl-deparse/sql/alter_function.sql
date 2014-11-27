--
-- ALTER_FUNCTION
--

-- XXX ALTER FUNCTION ... RENAME is "not yet supported"
-- ALTER FUNCTION plpgsql_function()
--   RENAME TO plpgsql_function_renamed;

-- XXX ALTER FUNCTION ... SET SCHEMA is "unimplemented deparse"
-- ALTER FUNCTION c_function_test ()
--   SET SCHEMA foo;

ALTER FUNCTION plpgsql_function_complex()
  COST 10;

ALTER FUNCTION plpgsql_function_srf(TEXT)
  LEAKPROOF
  SECURITY DEFINER
  ROWS 3
  STRICT
  SET enable_bitmapscan = FALSE;

ALTER FUNCTION plpgsql_function_srf(TEXT)
  RESET ALL;

