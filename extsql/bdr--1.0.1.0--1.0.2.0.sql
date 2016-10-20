--
-- We used to overload bdr_create_conflict_handler at the C level by
-- checking PG_NARGS but this was messy when replicating. Instead
-- overload at the SQL level with a default argument.
--
DROP FUNCTION bdr.bdr_create_conflict_handler(REGCLASS, NAME, REGPROCEDURE, bdr.bdr_conflict_type);

CREATE OR REPLACE FUNCTION bdr.bdr_create_conflict_handler(
    ch_rel REGCLASS,
    ch_name NAME,
    ch_proc REGPROCEDURE,
    ch_type bdr.bdr_conflict_type,
    ch_timeframe INTERVAL DEFAULT NULL
)
RETURNS VOID
LANGUAGE C
AS 'MODULE_PATHNAME';
