--
-- CREATE_TABLE
--

-- Datatypes
CREATE TABLE datatype_table (
    id             SERIAL,
    id_big         BIGSERIAL,
    v_smallint     SMALLINT,
    v_int          INT,
    v_bigint       BIGINT,
    v_char         CHAR(1),
    v_varchar      VARCHAR(10),
    v_text         TEXT,
    v_bool         BOOLEAN,
    v_inet         INET,
    v_numeric      NUMERIC(1,0),
    v_float        FLOAT(1),
    v_tsvector     TSVECTOR,
    v_timestamp    TIMESTAMP,
    v_timestamp_tz TIMESTAMP WITH TIME ZONE,
    v_point        POINT,
    v_enum         ENUM_TEST,
    v_postal_code  japanese_postal_code,
    v_int2range    int2range,
    PRIMARY KEY (id),
    UNIQUE (id_big)
);

-- Constraint definitions

CREATE TABLE fkey_table (
    id           INT NOT NULL DEFAULT nextval('fkey_table_seq'::REGCLASS),
    datatype_id  INT NOT NULL REFERENCES datatype_table(id),
    big_id       BIGINT NOT NULL,
    PRIMARY KEY  (id),
    CONSTRAINT fkey_big_id
      FOREIGN KEY (big_id)
      REFERENCES datatype_table(id_big)
);


-- Inheritance
CREATE TABLE person (
    id          INT NOT NULL PRIMARY KEY,
	name 		text,
	age			int4,
	location 	point
);

CREATE TABLE emp (
	salary 		int4,
	manager 	name
) INHERITS (person) WITH OIDS;


CREATE TABLE student (
	gpa 		float8
) INHERITS (person);

CREATE TABLE stud_emp (
	percent 	int4
) INHERITS (emp, student);


-- Storage parameters

CREATE TABLE storage (
    id INT
) WITH (
    fillfactor = 10,
    autovacuum_enabled = FALSE
);

-- LIKE

CREATE TABLE like_datatype_table (
  LIKE datatype_table
  EXCLUDING ALL
);

CREATE TABLE like_fkey_table (
  LIKE fkey_table
  INCLUDING DEFAULTS
  INCLUDING INDEXES
  INCLUDING STORAGE
);


-- Volatile table types
CREATE UNLOGGED TABLE unlogged_table (
    id INT PRIMARY KEY
);

CREATE TEMP TABLE temp_table (
    id INT PRIMARY KEY
);

CREATE TEMP TABLE temp_table_commit_delete (
    id INT PRIMARY KEY
)
ON COMMIT DELETE ROWS;

CREATE TEMP TABLE temp_table_commit_drop (
    id INT PRIMARY KEY
)
ON COMMIT DROP;
