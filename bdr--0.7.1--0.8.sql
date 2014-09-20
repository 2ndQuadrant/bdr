ALTER TYPE bdr.bdr_conflict_resolution ADD VALUE 'apply_change' BEFORE 'unhandled_tx_abort';
ALTER TYPE bdr.bdr_conflict_resolution ADD VALUE 'skip_change' BEFORE 'unhandled_tx_abort';

CREATE OR REPLACE FUNCTION bdr.bdr_add_truncate_trigger()
RETURNS event_trigger
LANGUAGE C
AS 'MODULE_PATHNAME'
;

CREATE EVENT TRIGGER bdr_add_truncate_trigger
ON ddl_command_end
EXECUTE PROCEDURE bdr.bdr_add_truncate_trigger();

DO $DO$BEGIN
IF right(bdr_version(), 4) = '-udr' THEN

CREATE OR REPLACE FUNCTION bdr.bdr_replicate_ddl_command(cmd TEXT)
RETURNS VOID
LANGUAGE C
AS 'MODULE_PATHNAME'
;

END IF;
END;$DO$;