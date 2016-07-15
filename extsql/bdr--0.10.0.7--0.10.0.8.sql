SET LOCAL search_path = bdr;
SET bdr.permit_unsafe_ddl_commands = true;
SET bdr.skip_ddl_replication = true;

CREATE OR REPLACE FUNCTION bdr.queue_truncate()
    RETURNS TRIGGER
	LANGUAGE C
	AS 'MODULE_PATHNAME','bdr_queue_truncate';
;

ALTER EVENT TRIGGER bdr_truncate_trigger_add ENABLE ALWAYS;


RESET bdr.permit_unsafe_ddl_commands;
RESET bdr.skip_ddl_replication;
RESET search_path;
