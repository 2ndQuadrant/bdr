CREATE FUNCTION pg_catalog.pg_td_mkdir(text)
RETURNS void
AS '$libdir/test_decoding_regsupport', 'pg_td_mkdir'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION pg_catalog.pg_td_rmdir(text)
RETURNS void
AS '$libdir/test_decoding_regsupport', 'pg_td_rmdir'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION pg_catalog.pg_td_unlink(text)
RETURNS void
AS '$libdir/test_decoding_regsupport', 'pg_td_unlink'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION pg_catalog.pg_td_chmod(text, text)
RETURNS void
AS '$libdir/test_decoding_regsupport', 'pg_td_chmod'
LANGUAGE C VOLATILE STRICT;

-- drop all slots
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE slot_name LIKE 'slottest_%';

-- succeed
SELECT * FROM pg_create_physical_replication_slot('slottest_1');
SELECT * FROM pg_create_physical_replication_slot('slottest_2');
-- fail, duplicate
SELECT * FROM pg_create_physical_replication_slot('slottest_1');
-- succeed
SELECT pg_drop_replication_slot('slottest_1');

SELECT pg_td_chmod('pg_replslot', '0');
-- fail, no permissions on entire pg_replslot director
SELECT * FROM pg_create_physical_replication_slot('slottest_3');
SELECT * FROM pg_drop_replication_slot('slottest_2');

SELECT pg_td_chmod('pg_replslot', '700');

-- succeed, we have permissions again
SELECT * FROM pg_drop_replication_slot('slottest_2');

SELECT * FROM pg_create_physical_replication_slot('slottest_4');
SELECT pg_td_chmod('pg_replslot/slottest_4', '000');
-- succeed as the rename works, with WARNINGs
SELECT * FROM pg_drop_replication_slot('slottest_4');
-- fail, temp director exists
SELECT * FROM pg_create_physical_replication_slot('slottest_4');
-- cleanup half-deleted slot
SELECT pg_td_chmod('pg_replslot/slottest_4.tmp', '700');
SELECT pg_td_unlink('pg_replslot/slottest_4.tmp/state');
SELECT pg_td_rmdir('pg_replslot/slottest_4.tmp');
-- check cleanup
SELECT * FROM pg_create_physical_replication_slot('slottest_4');
SELECT * FROM pg_drop_replication_slot('slottest_4');

-- fail, slot directory exists
SELECT pg_td_mkdir('pg_replslot/slottest_5');
SELECT * FROM pg_drop_replication_slot('slottest_5');
SELECT pg_td_rmdir('pg_replslot/slottest_5');

SELECT pg_td_mkdir('pg_replslot/slottest_6.tmp');
SELECT pg_td_mkdir('pg_replslot/slottest_6.tmp/blub');

-- suceed, temp directory exists, but we remove it
SELECT * FROM pg_create_physical_replication_slot('slottest_6');

-- succeed, temp directory removed
SELECT * FROM pg_create_physical_replication_slot('slottest_6');
SELECT * FROM pg_drop_replication_slot('slottest_6');

SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE slot_name LIKE 'slottest_%';
