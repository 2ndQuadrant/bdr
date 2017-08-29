\echo Use "CREATE EXTENSION bdr" to load this file. \quit

CREATE SCHEMA bdr;

REVOKE ALL ON SCHEMA bdr FROM public;

-- Necessary because we'll reference triggers and so on:
GRANT USAGE ON SCHEMA bdr TO public;

-- BDR wants to be loaded by pglogical as a plugin, so it must register its
-- self with pglogical.
INSERT INTO pglogical.plugins (plugin_module, plugin_entrypoint)
VALUES ('bdr','bdr_init_pgl_plugin');

CREATE TABLE bdr.node_group
(
	node_group_id oid NOT NULL PRIMARY KEY,
	node_group_name name NOT NULL UNIQUE
);

REVOKE ALL ON bdr.node_group FROM public;

CREATE TABLE bdr.node
(
	pglogical_node_id oid NOT NULL PRIMARY KEY,
	node_group_id oid NOT NULL REFERENCES bdr.node_group(node_group_id)
);

REVOKE ALL ON bdr.node FROM public;

-- This just records which subscriptions are ours. Since we use
-- pglogical node IDs and the subscription references those, we
-- need not record anything else.
CREATE TABLE bdr.node_subscriptions
(
	pglogical_subscription_id oid PRIMARY KEY
);

REVOKE ALL ON bdr.node_subscriptions FROM public;

CREATE TABLE bdr.node_peer_progress
(
	subscriber_node_id oid,
	provider_node_id oid,
	PRIMARY KEY (subscriber_node_id, provider_node_id),
	last_update_sent_time timestamptz NOT NULL,
	last_update_recv_time timestamptz NOT NULL,
	last_update_insert_lsn pg_lsn NOT NULL,
	replay_position pg_lsn NOT NULL,
	received_parted_confirm boolean NOT NULL,
	subscriber_provider_state text NOT NULL
);

REVOKE ALL ON bdr.node_peer_progress FROM public;

CREATE TABLE bdr.node_part_progress
(
	parting_node_id oid PRIMARY KEY,
	furthest_peer_id oid NOT NULL,
	part_type "char" NOT NULL,
	npeers_confirmed integer NOT NULL
);

REVOKE ALL ON bdr.node_part_progress FROM public;

CREATE TABLE bdr.distributed_message_journal
(
	global_consensus_no integer NOT NULL PRIMARY KEY,
	originator_id oid NOT NULL,
	originator_state_no integer NOT NULL,
	UNIQUE(originator_id, originator_state_no),
	originator_sendtime_lsn pg_lsn NOT NULL,
	consensus_majority_ok boolean NOT NULL,
	message_type "char" NOT NULL,
	message bytea NOT NULL
);

REVOKE ALL ON bdr.distributed_message_journal FROM public;

CREATE FUNCTION bdr.decode_message_payload(message_type "char", message_payload bytea)
RETURNS text LANGUAGE c AS 'MODULE_PATHNAME','bdr_decode_message_payload';

REVOKE ALL ON FUNCTION bdr.decode_message_payload("char", bytea) FROM public;

CREATE TABLE bdr.state_journal
(
	state_counter integer NOT NULL PRIMARY KEY,
	state integer NOT NULL,
	state_data bytea,
	global_consensus_no integer
);

REVOKE ALL ON bdr.state_journal FROM public;

CREATE FUNCTION bdr.decode_state(state integer, state_data bytea)
RETURNS text LANGUAGE c AS 'MODULE_PATHNAME','bdr_decode_state';

/*
 * BDR node manipulation
 */
CREATE FUNCTION bdr.create_node(node_name text, local_dsn text)
RETURNS oid LANGUAGE c AS 'MODULE_PATHNAME','bdr_create_node_sql';

COMMENT ON FUNCTION bdr.create_node(text, text) IS
'Create a new local BDR node';

CREATE FUNCTION bdr.create_node_group(node_group_name text)
RETURNS oid LANGUAGE c AS 'MODULE_PATHNAME','bdr_create_nodegroup_sql';

COMMENT ON FUNCTION bdr.create_node_group(text) IS
'Create a new local BDR node group and make the local node the first member';

CREATE FUNCTION bdr.replication_set_add_table(relation regclass, set_name text, synchronize_data boolean DEFAULT false,
	columns text[] DEFAULT NULL, row_filter text DEFAULT NULL)
RETURNS boolean CALLED ON NULL INPUT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'bdr_replication_set_add_table';

CREATE FUNCTION bdr.replication_set_remove_table(relation regclass, set_name text)
RETURNS boolean STRICT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'bdr_replication_set_remove_table';

/*
 * Interface for BDR message broker
 */
CREATE FUNCTION bdr.msgb_connect(origin_node oid, destination_node oid, last_sent_msgid oid)
RETURNS void LANGUAGE c AS 'MODULE_PATHNAME','msgb_connect';

REVOKE ALL ON FUNCTION bdr.msgb_connect(oid,oid,oid) FROM public;

CREATE FUNCTION bdr.msgb_deliver_message(destination_node oid, message_id oid, payload bytea)
RETURNS void LANGUAGE c AS 'MODULE_PATHNAME','msgb_deliver_message';

REVOKE ALL ON FUNCTION bdr.msgb_deliver_message(oid,oid,bytea) FROM public;
