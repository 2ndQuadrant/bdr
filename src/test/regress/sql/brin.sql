CREATE TABLE brintest (byteacol bytea,
	charcol "char",
	namecol name,
	int8col bigint,
	int2col smallint,
	int4col integer,
	textcol text,
	oidcol oid,
	tidcol tid,
	float4col real,
	float8col double precision,
	macaddrcol macaddr,
	inetcol inet,
	bpcharcol character,
	datecol date,
	timecol time without time zone,
	timestampcol timestamp without time zone,
	timestamptzcol timestamp with time zone,
	intervalcol interval,
	timetzcol time with time zone,
	bitcol bit(10),
	varbitcol bit varying(16),
	numericcol numeric,
	uuidcol uuid,
	lsncol pg_lsn
) WITH (fillfactor=10);

INSERT INTO brintest SELECT
	repeat(stringu1, 42)::bytea,
	substr(stringu1, 1, 1)::"char",
	stringu1::name, 142857 * tenthous,
	thousand,
	twothousand,
	repeat(stringu1, 42),
	unique1::oid,
	format('(%s,%s)', tenthous, twenty)::tid,
	(four + 1.0)/(hundred+1),
	odd::float8 / (tenthous + 1),
	format('%s:00:%s:00:%s:00', to_hex(odd), to_hex(even), to_hex(hundred))::macaddr,
	inet '10.2.3.4' + tenthous,
	substr(stringu1, 1, 1)::bpchar,
	date '1995-08-15' + tenthous,
	time '01:20:30' + thousand * interval '18.5 second',
	timestamp '1942-07-23 03:05:09' + tenthous * interval '36.38 hours',
	timestamptz '1972-10-10 03:00' + thousand * interval '1 hour',
	justify_days(justify_hours(tenthous * interval '12 minutes')),
	timetz '01:30:20' + hundred * interval '15 seconds',
	thousand::bit(10),
	tenthous::bit(16)::varbit,
	tenthous::numeric(36,30) * fivethous * even / (hundred + 1),
	format('%s%s-%s-%s-%s-%s%s%s', to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'))::uuid,
	format('%s/%s%s', odd, even, tenthous)::pg_lsn
FROM tenk1 LIMIT 5;

-- throw in some NULL-only tuples too
INSERT INTO brintest SELECT NULL FROM tenk1 LIMIT 25;

CREATE INDEX brinidx ON brintest USING brin (
	byteacol,
	charcol,
	namecol,
	int8col,
	int2col,
	int4col,
	textcol,
	oidcol,
	tidcol,
	float4col,
	float8col,
	macaddrcol,
	inetcol inet_minmax_ops,
	bpcharcol,
	datecol,
	timecol,
	timestampcol,
	timestamptzcol,
	intervalcol,
	timetzcol,
	bitcol,
	varbitcol,
	numericcol,
	uuidcol,
	lsncol
) with (pages_per_range = 1);

BEGIN;
CREATE TABLE brinopers (colname name, op text[], value text[],
	check (cardinality(op) = cardinality(value)));

INSERT INTO brinopers VALUES ('byteacol', '{>, >=, =, <=, <}', '{ZZAAAA, ZZAAAA, AAAAAA, AAAAAA, AAAAAA}');
INSERT INTO brinopers VALUES ('charcol', '{>, >=, =, <=, <}', '{Z, Z, A, A, A}');
INSERT INTO brinopers VALUES ('namecol', '{>, >=, =, <=, <}', '{ZZAAAA, ZZAAAA, AAAAAA, AAAAAA, AAAAAA}');
INSERT INTO brinopers VALUES ('int8col', '{>, >=, =, <=, <}', '{1428427143, 1428427143, 0, 0, 0}');
INSERT INTO brinopers VALUES ('int2col', '{>, >=, =, <=, <}', '{999, 999, 0, 0, 0}');
INSERT INTO brinopers VALUES ('int4col', '{>, >=, =, <=, <}', '{1999, 1999, 0, 0, 0}');
INSERT INTO brinopers VALUES ('textcol', '{>, >=, =, <=, <}', '{ZZAAAA, ZZAAAA, AAAAA, AAAAA, AAAAA}');
INSERT INTO brinopers VALUES ('oidcol', '{>, >=, =, <=, <}', '{9999, 9999, 0, 0, 0}');
INSERT INTO brinopers VALUES ('tidcol', '{>, >=, =, <=, <}', '{"(9999,19)", "(9999,19)", "(0,0)", "(0,0)", "(0,0)"}');
INSERT INTO brinopers VALUES ('float4col', '{>, >=, =, <=, <}', '{1, 1, 0.0103093, 0.0103093, 0.0103093}');
INSERT INTO brinopers VALUES ('float8col', '{>, >=, =, <=, <}', '{1.98, 1.98, 0, 0, 0}');
INSERT INTO brinopers VALUES ('inetcol', '{>, >=, =, <=, <}', '{10.2.42.19, 10.2.42.19, 10.2.3.4, 10.2.3.4, 10.2.3.4}');
INSERT INTO brinopers VALUES ('bpcharcol', '{>, >=, =, <=, <}', '{Z, Z, A, A, A}');
INSERT INTO brinopers VALUES ('datecol', '{>, >=, =, <=, <}', '{2022-12-30, 2022-12-30, 1995-08-15, 1995-08-15, 1995-08-15}');
INSERT INTO brinopers VALUES ('timecol', '{>, >=, =, <=, <}', '{06:28:31.5, 06:28:31.5, 01:20:30, 01:20:30, 01:20:30}');
INSERT INTO brinopers VALUES ('timestampcol', '{>, >=, =, <=, <}', '{1984-01-20 22:42:21, 1984-01-20 22:42:21, 1942-07-23 03:05:09, 1942-07-23 03:05:09, 1942-07-23 03:05:09}');
INSERT INTO brinopers VALUES ('timestamptzcol', '{>, >=, =, <=, <}', '{1972-11-20 19:00:00-03, 1972-11-20 19:00:00-03, 1972-10-10 03:00:00-04, 1972-10-10 03:00:00-04, 1972-10-10 03:00:00-04}');
INSERT INTO brinopers VALUES ('intervalcol', '{>, >=, =, <=, <}', '{2 mons 23 days 07:48:00, 2 mons 23 days 07:48:00, 00:00:00, 00:00:00, 00:00:00}');
INSERT INTO brinopers VALUES ('timetzcol', '{>, >=, =, <=, <}', '{01:55:05-03, 01:55:05-03, 01:30:20-03, 01:30:20-03, 01:30:20-03}');
INSERT INTO brinopers VALUES ('numericcol', '{>, >=, =, <=, <}', '{99470151.9, 99470151.9, 0.00, 0.01, 0.01}');
INSERT INTO brinopers VALUES ('macaddrcol', '{>, >=, =, <=, <}', '{ff:fe:00:00:00:00, ff:fe:00:00:00:00, 00:00:01:00:00:00, 00:00:01:00:00:00, 00:00:01:00:00:00}');
INSERT INTO brinopers VALUES ('bitcol', '{>, >=, =, <=, <}', '{1111111000, 1111111000, 0000000010, 0000000010, 0000000010}');
INSERT INTO brinopers VALUES ('varbitcol', '{>, >=, =, <=, <}', '{1111111111111000, 1111111111111000, 0000000000000100, 0000000000000100, 0000000000000100}');
INSERT INTO brinopers VALUES ('uuidcol', '{>, >=, =, <=, <}', '{99989998-9998-9998-9998-999899989998, 99989998-9998-9998-9998-999899989998, 00040004-0004-0004-0004-000400040004, 00040004-0004-0004-0004-000400040004, 00040004-0004-0004-0004-000400040005}');
INSERT INTO brinopers VALUES ('lsncol', '{>, >=, =, <=, <, IS, IS NOT}', '{198/1999799, 198/1999799, 30/312815, 0/1200, 0/1200, NULL, NULL}');
COMMIT;

DO $x$
DECLARE
        r record;
        tabname text;
        tabname_ss text;
		count int;
		query text;
		plan text;
BEGIN
        FOR r IN SELECT row_number() OVER (), colname, oper, value[ordinality] FROM brinopers, unnest(op) WITH ORDINALITY AS oper LOOP
                tabname := format('qry_%s', r.row_number);
                tabname_ss := tabname || '_ss';
				query = format($y$INSERT INTO %s SELECT ctid FROM brintest WHERE %s %s %L $y$,
                        tabname, r.colname, r.oper, r.value);
				-- run the query using the brin index
                SET enable_seqscan = 0;
                SET enable_bitmapscan = 1;
                EXECUTE format('create temp table %s (tid tid) ON COMMIT DROP', tabname);
                EXECUTE query;

				-- run the query using a seqscan
                SET enable_seqscan = 1;
                SET enable_bitmapscan = 0;
				query = format($y$INSERT INTO %s SELECT ctid FROM brintest WHERE %s %s %L $y$,
                        tabname_ss, r.colname, r.oper, r.value);
                EXECUTE format('create temp table %s (tid tid) ON COMMIT DROP', tabname_ss);
                EXECUTE query;

				-- make sure both return the same results
                EXECUTE format('SELECT * from %s EXCEPT ALL SELECT * FROM %s', tabname, tabname_ss);
				GET DIAGNOSTICS count = ROW_COUNT;
                IF count <> 0 THEN RAISE EXCEPTION 'something not right in %: count %', r, count; END IF;
                EXECUTE format('SELECT * from %s EXCEPT ALL SELECT * FROM %s', tabname_ss, tabname);
				GET DIAGNOSTICS count = ROW_COUNT;
                IF count <> 0 THEN RAISE EXCEPTION 'something not right in %: count %', r, count; END IF;
        end loop;
end;
$x$;

INSERT INTO brintest SELECT
	repeat(stringu1, 42)::bytea,
	substr(stringu1, 1, 1)::"char",
	stringu1::name, 142857 * tenthous,
	thousand,
	twothousand,
	repeat(stringu1, 42),
	unique1::oid,
	format('(%s,%s)', tenthous, twenty)::tid,
	(four + 1.0)/(hundred+1),
	odd::float8 / (tenthous + 1),
	format('%s:00:%s:00:%s:00', to_hex(odd), to_hex(even), to_hex(hundred))::macaddr,
	inet '10.2.3.4' + tenthous,
	substr(stringu1, 1, 1)::bpchar,
	date '1995-08-15' + tenthous,
	time '01:20:30' + thousand * interval '18.5 second',
	timestamp '1942-07-23 03:05:09' + tenthous * interval '36.38 hours',
	timestamptz '1972-10-10 03:00' + thousand * interval '1 hour',
	justify_days(justify_hours(tenthous * interval '12 minutes')),
	timetz '01:30:20' + hundred * interval '15 seconds',
	thousand::bit(10),
	tenthous::bit(16)::varbit,
	tenthous::numeric(36,30) * fivethous * even / (hundred + 1),
	format('%s%s-%s-%s-%s-%s%s%s', to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'))::uuid,
	format('%s/%s%s', odd, even, tenthous)::pg_lsn
FROM tenk1 LIMIT 5 OFFSET 5;

SELECT brin_summarize_new_values('brinidx'::regclass);

UPDATE brintest SET int8col = int8col * int4col;
UPDATE brintest SET textcol = '' WHERE textcol IS NOT NULL;
