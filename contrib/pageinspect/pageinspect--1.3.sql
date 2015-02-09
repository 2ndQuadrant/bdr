/* contrib/pageinspect/pageinspect--1.3.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pageinspect" to load this file. \quit

--
-- get_raw_page()
--
CREATE FUNCTION get_raw_page(text, int4)
RETURNS bytea
AS 'MODULE_PATHNAME', 'get_raw_page'
LANGUAGE C STRICT;

CREATE FUNCTION get_raw_page(text, text, int4)
RETURNS bytea
AS 'MODULE_PATHNAME', 'get_raw_page_fork'
LANGUAGE C STRICT;

--
-- page_header()
--
CREATE FUNCTION page_header(IN page bytea,
    OUT lsn pg_lsn,
    OUT checksum smallint,
    OUT flags smallint,
    OUT lower smallint,
    OUT upper smallint,
    OUT special smallint,
    OUT pagesize smallint,
    OUT version smallint,
    OUT prune_xid xid)
AS 'MODULE_PATHNAME', 'page_header'
LANGUAGE C STRICT;

--
-- heap_page_items()
--
CREATE FUNCTION heap_page_items(IN page bytea,
    OUT lp smallint,
    OUT lp_off smallint,
    OUT lp_flags smallint,
    OUT lp_len smallint,
    OUT t_xmin xid,
    OUT t_xmax xid,
    OUT t_field3 int4,
    OUT t_ctid tid,
    OUT t_infomask2 integer,
    OUT t_infomask integer,
    OUT t_hoff smallint,
    OUT t_bits text,
    OUT t_oid oid)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'heap_page_items'
LANGUAGE C STRICT;

--
-- bt_metap()
--
CREATE FUNCTION bt_metap(IN relname text,
    OUT magic int4,
    OUT version int4,
    OUT root int4,
    OUT level int4,
    OUT fastroot int4,
    OUT fastlevel int4)
AS 'MODULE_PATHNAME', 'bt_metap'
LANGUAGE C STRICT;

--
-- bt_page_stats()
--
CREATE FUNCTION bt_page_stats(IN relname text, IN blkno int4,
    OUT blkno int4,
    OUT type "char",
    OUT live_items int4,
    OUT dead_items int4,
    OUT avg_item_size int4,
    OUT page_size int4,
    OUT free_size int4,
    OUT btpo_prev int4,
    OUT btpo_next int4,
    OUT btpo int4,
    OUT btpo_flags int4)
AS 'MODULE_PATHNAME', 'bt_page_stats'
LANGUAGE C STRICT;

--
-- bt_page_items()
--
CREATE FUNCTION bt_page_items(IN relname text, IN blkno int4,
    OUT itemoffset smallint,
    OUT ctid tid,
    OUT itemlen smallint,
    OUT nulls bool,
    OUT vars bool,
    OUT data text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bt_page_items'
LANGUAGE C STRICT;

--
-- brin_page_type()
--
CREATE FUNCTION brin_page_type(IN page bytea)
RETURNS text
AS 'MODULE_PATHNAME', 'brin_page_type'
LANGUAGE C STRICT;

--
-- brin_metapage_info()
--
CREATE FUNCTION brin_metapage_info(IN page bytea, OUT magic text,
	OUT version integer, OUT pagesperrange integer, OUT lastrevmappage bigint)
AS 'MODULE_PATHNAME', 'brin_metapage_info'
LANGUAGE C STRICT;

--
-- brin_revmap_data()
--
CREATE FUNCTION brin_revmap_data(IN page bytea,
	OUT pages tid)
RETURNS SETOF tid
AS 'MODULE_PATHNAME', 'brin_revmap_data'
LANGUAGE C STRICT;

--
-- brin_page_items()
--
CREATE FUNCTION brin_page_items(IN page bytea, IN index_oid regclass,
	OUT itemoffset int,
	OUT blknum int,
	OUT attnum int,
	OUT allnulls bool,
	OUT hasnulls bool,
	OUT placeholder bool,
	OUT value text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'brin_page_items'
LANGUAGE C STRICT;

--
-- fsm_page_contents()
--
CREATE FUNCTION fsm_page_contents(IN page bytea)
RETURNS text
AS 'MODULE_PATHNAME', 'fsm_page_contents'
LANGUAGE C STRICT;

--
-- GIN functions
--

--
-- gin_metapage_info()
--
CREATE FUNCTION gin_metapage_info(IN page bytea,
    OUT pending_head bigint,
    OUT pending_tail bigint,
    OUT tail_free_size int4,
    OUT n_pending_pages bigint,
    OUT n_pending_tuples bigint,
    OUT n_total_pages bigint,
    OUT n_entry_pages bigint,
    OUT n_data_pages bigint,
    OUT n_entries bigint,
    OUT version int4)
AS 'MODULE_PATHNAME', 'gin_metapage_info'
LANGUAGE C STRICT;

--
-- gin_page_opaque_info()
--
CREATE FUNCTION gin_page_opaque_info(IN page bytea,
    OUT rightlink bigint,
    OUT maxoff int4,
    OUT flags text[])
AS 'MODULE_PATHNAME', 'gin_page_opaque_info'
LANGUAGE C STRICT;

--
-- gin_leafpage_items()
--
CREATE FUNCTION gin_leafpage_items(IN page bytea,
    OUT first_tid tid,
    OUT nbytes int2,
    OUT tids tid[])
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'gin_leafpage_items'
LANGUAGE C STRICT;
