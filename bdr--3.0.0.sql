\echo Use "CREATE EXTENSION bdr" to load this file. \quit

CREATE SCHEMA bdr;

REVOKE ALL ON SCHEMA bdr FROM public;

-- Necessary because we'll reference triggers and so on:
GRANT USAGE ON SCHEMA bdr TO public;

CREATE TABLE bdr.node_group
(
	node_group_id oid NOT NULL PRIMARY KEY,
	node_group_name name NOT NULL UNIQUE,
	node_group_default_repset oid NOT NULL
) WITH (user_catalog_table=true);

REVOKE ALL ON bdr.node_group FROM public;

CREATE TABLE bdr.node
(
	pglogical_node_id oid NOT NULL PRIMARY KEY,
	node_group_id oid NOT NULL REFERENCES bdr.node_group(node_group_id),
	local_state oid NOT NULL,
	seq_id integer NOT NULL,
	confirmed_our_join bool NOT NULL,
	dbname name NOT NULL
) WITH (user_catalog_table=true);

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
    state_counter oid NOT NULL PRIMARY KEY,
    state oid NOT NULL,
    global_consensus_no bigint,
    state_extra_data bytea
) WITH (user_catalog_table=true);

REVOKE ALL ON bdr.state_journal FROM public;

CREATE FUNCTION bdr.decode_state(state integer, state_data bytea)
RETURNS text LANGUAGE c AS 'MODULE_PATHNAME','bdr_decode_state';

/*
 * BDR node manipulation
 */
CREATE FUNCTION bdr.create_node(node_name text, local_dsn text)
RETURNS oid CALLED ON NULL INPUT VOLATILE
LANGUAGE c AS 'MODULE_PATHNAME','bdr_create_node_sql';

COMMENT ON FUNCTION bdr.create_node(text, text) IS
'Create a new local BDR node';

CREATE FUNCTION bdr.create_node_group(node_group_name text)
RETURNS oid CALLED ON NULL INPUT VOLATILE
LANGUAGE c AS 'MODULE_PATHNAME','bdr_create_node_group_sql';

COMMENT ON FUNCTION bdr.create_node_group(text) IS
'Create a new local BDR node group and make the local node the first member';

CREATE FUNCTION bdr.join_node_group(join_target_dsn text, node_group_name text)
RETURNS void CALLED ON NULL INPUT VOLATILE
LANGUAGE c AS 'MODULE_PATHNAME','bdr_join_node_group_sql';

COMMENT ON FUNCTION bdr.join_node_group(text,text) IS
'Join an existing BDR node group on peer at ''dsn''';

-- Dirty hack required to finish a join for now,
-- until we split it up and move into manager
CREATE FUNCTION bdr.join_node_group_finish()
RETURNS void CALLED ON NULL INPUT VOLATILE
LANGUAGE c AS 'MODULE_PATHNAME','bdr_join_node_group_finish_sql';

CREATE FUNCTION bdr.replication_set_add_table(relation regclass, set_name text DEFAULT NULL, synchronize_data boolean DEFAULT false,
	columns text[] DEFAULT NULL, row_filter text DEFAULT NULL)
RETURNS void CALLED ON NULL INPUT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'bdr_replication_set_add_table';

CREATE FUNCTION bdr.replication_set_remove_table(relation regclass, set_name text DEFAULT NULL)
RETURNS void CALLED ON NULL INPUT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'bdr_replication_set_remove_table';

/*
 * Interface for BDR message broker
 */
CREATE FUNCTION bdr.msgb_connect(origin_node oid, destination_node oid, last_sent_msgid oid)
RETURNS void LANGUAGE c AS 'MODULE_PATHNAME','msgb_connect';

REVOKE ALL ON FUNCTION bdr.msgb_connect(oid,oid,oid) FROM public;

CREATE FUNCTION bdr.msgb_deliver_message(destination_node oid, message_id oid, payload bytea)
RETURNS void LANGUAGE c AS 'MODULE_PATHNAME','msgb_deliver_message';

REVOKE ALL ON FUNCTION bdr.msgb_deliver_message(oid,oid,bytea) FROM public;

/*
 * Functions used to query remote node info. Don't change these without caution,
 * as they're relied on by node join.
 */
CREATE TYPE bdr.node_info_type AS
(
	node_id oid,
	node_name text,
	node_local_state oid,
	node_seq_id integer,
	nodegroup_id oid,
	nodegroup_name text,
	pgl_interface_id oid,
	pgl_interface_name text,
	pgl_interface_dsn text,
	bdr_dbname text
);

CREATE FUNCTION bdr.local_node_info()
RETURNS bdr.node_info_type
VOLATILE LANGUAGE c AS 'MODULE_PATHNAME','bdr_local_node_info_sql';

CREATE FUNCTION bdr.node_group_member_info(nodegroup_id oid)
RETURNS SETOF bdr.node_info_type
CALLED ON NULL INPUT VOLATILE
LANGUAGE c AS 'MODULE_PATHNAME','bdr_node_group_member_info';

CREATE FUNCTION bdr.internal_submit_join_request(nodegroup_name text,
	joining_node_name text, joining_node_id oid, joining_node_state oid,
	joining_node_if_name text, joining_node_if_id oid,
	joining_node_if_connstr text, joining_node_dbname text)
RETURNS text /* actually uint64 */
CALLED ON NULL INPUT VOLATILE
LANGUAGE c AS 'MODULE_PATHNAME','bdr_internal_submit_join_request';

/*
 * Helper views
 */
CREATE VIEW bdr.node_group_replication_sets AS
SELECT g.node_group_name, l.node_name, s.set_name
FROM bdr.node n
LEFT JOIN bdr.node_group g ON (g.node_group_id = g.node_group_id)
LEFT JOIN pglogical.node l ON (n.pglogical_node_id = l.node_id)
LEFT JOIN pglogical.replication_set s ON (s.set_nodeid = l.node_id)
WHERE s.set_isinternal
ORDER BY set_name;

COMMENT ON VIEW bdr.node_group_replication_sets IS
'BDR replication sets for local node groups';
