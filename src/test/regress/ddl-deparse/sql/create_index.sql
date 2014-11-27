---
--- CREATE_INDEX
---

CREATE INDEX test_index_1 ON datatype_table (v_smallint);
CREATE INDEX test_index_2 ON datatype_table (v_smallint, v_int);
CREATE INDEX test_index_3 ON datatype_table (v_int) WHERE v_int > 100;
CREATE UNIQUE INDEX test_index_4 ON datatype_table (v_bigint);
CREATE INDEX CONCURRENTLY test_index_5 ON datatype_table (v_int);
CREATE INDEX test_index_6 ON datatype_table (v_text) WITH (fillfactor=50);
CREATE INDEX test_index_7 ON datatype_table (v_text COLLATE "ja_JP");
CREATE INDEX test_index_8 ON datatype_table (LOWER(v_text));
CREATE INDEX test_index_9 ON datatype_table (v_smallint NULLS FIRST);
CREATE INDEX test_index_10 ON datatype_table (v_bigint DESC);
CREATE INDEX test_index_11 ON datatype_table USING btree (v_smallint);

CREATE INDEX test_gin_1 ON datatype_table USING gin(v_tsvector);
CREATE INDEX test_gin_2 ON datatype_table USING gin(v_tsvector) WITH (fastupdate=off);

CREATE INDEX test_gist_1 ON datatype_table USING gist(v_tsvector) WITH (buffering=on);

CREATE INDEX test_hash_1 ON datatype_table USING hash (v_text);


