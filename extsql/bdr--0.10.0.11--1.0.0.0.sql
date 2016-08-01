CREATE FUNCTION bdr.bdr_is_active_in_db()
RETURNS boolean LANGUAGE c
AS 'MODULE_PATHNAME','bdr_is_active_in_db';
