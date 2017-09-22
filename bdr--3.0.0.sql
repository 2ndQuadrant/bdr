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

-- This records which subscriptions are owned by the local BDR node
-- (target_node_id) and the mode they are in.
--
-- Since we use pglogical node IDs and the subscription references those,
-- strictly we need not record anything else. The subscription's replication
-- sets tell us which nodegroup it's part of.
--
-- That makes filtering and lookups a real pain, though, so we store the source
-- and destination node and nodegroup id here.
--
CREATE TABLE bdr.subscription
(
	pgl_subscription_id oid PRIMARY KEY,
	nodegroup_id oid NOT NULL,
	origin_node_id oid NOT NULL,
	target_node_id oid NOT NULL,
	UNIQUE(nodegroup_id, origin_node_id, target_node_id),
	subscription_mode "char" NOT NULL CHECK (subscription_mode IN ('n', 'c', 'f'))
);

REVOKE ALL ON bdr.subscription FROM public;

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
    entered_time timestamptz NOT NULL,
    global_consensus_no bigint NOT NULL,
    peer_id oid NOT NULL,
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

CREATE FUNCTION bdr.join_node_group(join_target_dsn text, node_group_name text DEFAULT NULL)
RETURNS void CALLED ON NULL INPUT VOLATILE
LANGUAGE c AS 'MODULE_PATHNAME','bdr_join_node_group_sql';

COMMENT ON FUNCTION bdr.join_node_group(text,text) IS
'Join an existing BDR node group on peer at ''dsn''';

CREATE FUNCTION bdr.replication_set_add_table(relation regclass, set_name text DEFAULT NULL, synchronize_data boolean DEFAULT false,
	columns text[] DEFAULT NULL, row_filter text DEFAULT NULL)
RETURNS void CALLED ON NULL INPUT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'bdr_replication_set_add_table';

CREATE FUNCTION bdr.replication_set_remove_table(relation regclass, set_name text DEFAULT NULL)
RETURNS void CALLED ON NULL INPUT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'bdr_replication_set_remove_table';

CREATE FUNCTION bdr.replicate_ddl_command(ddl_cmd text, replication_sets text[] DEFAULT NULL)
RETURNS bool CALLED ON NULL INPUT VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'bdr_replicate_ddl_command';

CREATE FUNCTION bdr.gen_slot_name(dbname text, nodegroup_name text, origin_node_name text, target_node_name text)
RETURNS text STRICT STABLE LANGUAGE c AS 'MODULE_PATHNAME','bdr_gen_slot_name_sql';

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
 * Consensus messaging status lookup. The argument is actually a uint64.
 */
CREATE FUNCTION bdr.consensus_message_outcome(handle text)
RETURNS integer STRICT LANGUAGE c AS 'MODULE_PATHNAME','bdr_consensus_message_outcome';

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
 * Helper views and functions
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

CREATE FUNCTION bdr.decode_state_entry(entry bdr.state_journal)
RETURNS TABLE (state_name text, extra_data text)
STRICT VOLATILE
LANGUAGE c AS 'MODULE_PATHNAME','bdr_decode_state';

CREATE VIEW bdr.state_journal_details AS
SELECT 
  j.state_counter,
  j.state,
  d.state_name,
  j.entered_time,
  j.peer_id,
  n.node_name AS peer_name,
  d.extra_data
FROM bdr.state_journal j
  CROSS JOIN LATERAL bdr.decode_state_entry(j) d
  LEFT JOIN pglogical.node n ON (n.node_id = j.peer_id);

COMMENT ON VIEW bdr.state_journal_details IS
'Human readable decoding of bdr.state_journal';

CREATE VIEW bdr.subscription_summary AS
SELECT 
       ng.node_group_name, 
       sub_name, 
       onode.node_name   AS origin_name, 
       tnode.node_name   AS target_name, 
       sub_enabled, 
       sub_slot_name, 
       sub_replication_sets, 
       sub_forward_origins, 
       sub_apply_delay, 
       subscription_mode AS bdr_subscription_mode,
       ss.status         AS subscription_status,
       ng.node_group_id, 
       sub_id, 
       onode.node_id     AS origin_id, 
       tnode.node_id     AS target_id
FROM   bdr.subscription bs 
INNER JOIN pglogical.subscription ps 
        ON ( bs.pgl_subscription_id = ps.sub_id ) 
INNER JOIN pglogical.node onode 
        ON ( onode.node_id = ps.sub_origin ) 
INNER JOIN pglogical.node tnode 
        ON ( tnode.node_id = ps.sub_target ) 
INNER JOIN bdr.node btnode 
        ON ( btnode.pglogical_node_id = tnode.node_id ) 
INNER JOIN bdr.node bonode 
        ON ( bonode.pglogical_node_id = onode.node_id ) 
INNER JOIN bdr.node_group ng 
        ON ( btnode.node_group_id = ng.node_group_id )
CROSS JOIN LATERAL pglogical.show_subscription_status(sub_name) ss;

COMMENT ON VIEW bdr.subscription_summary IS
'breakdown of subscriptions for the local node';

CREATE VIEW bdr.local_node_summary AS
SELECT
    n.node_name, 
    ng.node_group_name, 
    rs.set_name AS repset_name, 
    ni.if_name AS interface_name, 
    if_dsn AS interface_connstr, 
    ( 
             SELECT   state_name 
             FROM     bdr.state_journal_details 
             ORDER BY state_counter DESC limit 1
    ) AS cur_state_journal_state,
    seq_id AS node_seq_id, 
    dbname AS node_local_dbname, 
    array_to_string(ARRAY[
        CASE WHEN rs.replicate_insert THEN 'INSERT' END,
        CASE WHEN rs.replicate_update THEN 'UPDATE' END,
        CASE WHEN rs.replicate_delete THEN 'DELETE' END,
        CASE WHEN rs.replicate_truncate THEN 'TRUNCATE' END
    ], ',') AS set_repl_ops,
    n.node_id,
    ng.node_group_id,
    rs.set_id,
    ni.if_id
FROM       pglogical.local_node l 
INNER JOIN pglogical.node n
        ON (l.node_id = n.node_id) 
INNER JOIN bdr.node bn
        ON (l.node_id = bn.pglogical_node_id) 
INNER JOIN pglogical.node_interface ni
        ON (l.node_local_interface = ni.if_id) 
INNER JOIN bdr.node_group ng
        ON (bn.node_group_id = ng.node_group_id) 
INNER JOIN pglogical.replication_set rs
        ON (ng.node_group_default_repset = rs.set_id);

COMMENT ON VIEW bdr.local_node_summary IS
'Summary view of the local BDR node';

CREATE VIEW bdr.node_slots AS
SELECT rbn.dbname AS target_dbname,
       ng.node_group_name,
       ln.node_name AS origin_name,
       rn.node_name AS target_name,
       bdr_slots.slot_name AS bdr_slot_name,
       rs.*
FROM pglogical.local_node l
INNER JOIN pglogical.node ln
        ON (l.node_id = ln.node_id)
INNER JOIN bdr.node lbn
        ON (lbn.pglogical_node_id = ln.node_id)
INNER JOIN bdr.node rbn
        ON (rbn.pglogical_node_id <> lbn.pglogical_node_id)
INNER JOIN pglogical.node rn
        ON (rbn.pglogical_node_id = rn.node_id)
INNER JOIN bdr.node_group ng
        ON (ng.node_group_id = rbn.node_group_id
            AND ng.node_group_id = lbn.node_group_id)
/* slot name from provider PoV:  remote (subscriber) dbname, ngname, remote (subscriber) name, local (provider) name */
CROSS JOIN LATERAL bdr.gen_slot_name(rbn.dbname, ng.node_group_name, ln.node_name, rn.node_name) bdr_slots(slot_name)
FULL OUTER JOIN
  (SELECT *
   FROM pg_catalog.pg_replication_slots
   LEFT JOIN pg_catalog.pg_stat_replication
          ON (pg_replication_slots.active_pid = pg_stat_replication.pid)
   WHERE slot_name LIKE 'bdr_%'
     AND database = current_database()
  ) rs ON (rs.slot_name = bdr_slots.slot_name);

COMMENT ON VIEW bdr.node_slots
IS 'Summary view mapping local BDR nodes to replication slot state and replication progress';
