DO $$
BEGIN
	IF EXISTS(SELECT 1 FROM pg_catalog.pg_enum WHERE enumlabel = 'apply_change' AND enumtypid = 'bdr.bdr_conflict_resolution'::regtype) THEN
		RETURN;
	END IF;

	-- We can't use ALTER TYPE ... ADD inside transaction, so do it the hard way...
	ALTER TYPE bdr.bdr_conflict_resolution RENAME TO bdr_conflict_resolution_old;

	CREATE TYPE bdr.bdr_conflict_resolution AS ENUM
	(
		'conflict_trigger_skip_change',
		'conflict_trigger_returned_tuple',
		'last_update_wins_keep_local',
		'last_update_wins_keep_remote',
		'apply_change',
		'skip_change',
		'unhandled_tx_abort'
	);

	COMMENT ON TYPE bdr.bdr_conflict_resolution IS 'Resolution of a bdr conflict - if a conflict was resolved by a conflict trigger, by last-update-wins tests on commit timestamps, etc.';

	ALTER TABLE bdr.bdr_conflict_history ALTER COLUMN conflict_resolution TYPE bdr.bdr_conflict_resolution USING conflict_resolution::text::bdr.bdr_conflict_resolution;

	DROP TYPE bdr.bdr_conflict_resolution_old;
END;$$;

DO $$
BEGIN
	IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_proc WHERE proname = 'bdr_variant' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'bdr')) THEN
		CREATE OR REPLACE FUNCTION bdr.bdr_variant()
		RETURNS TEXT
		LANGUAGE C
		AS 'MODULE_PATHNAME';
	END IF;

	IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_proc WHERE proname = 'bdr_replicate_ddl_command' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'bdr')) THEN
		CREATE OR REPLACE FUNCTION bdr.bdr_replicate_ddl_command(cmd TEXT)
		RETURNS VOID
		LANGUAGE C
		AS 'MODULE_PATHNAME';
	END IF;

	IF NOT EXISTS(SELECT 1 FROM pg_catalog.pg_proc WHERE proname = 'bdr_truncate_trigger_add' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'bdr')) THEN
		CREATE OR REPLACE FUNCTION bdr.bdr_truncate_trigger_add()
		RETURNS event_trigger
		LANGUAGE C
		AS 'MODULE_PATHNAME';

		CREATE EVENT TRIGGER bdr_truncate_trigger_add
		ON ddl_command_end
		EXECUTE PROCEDURE bdr.bdr_truncate_trigger_add();
	END IF;
END;$$;
