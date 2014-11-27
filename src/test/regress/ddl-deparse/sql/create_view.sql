--
-- CREATE_VIEW
--

CREATE VIEW static_view AS
  SELECT 'foo'::TEXT AS col;

CREATE OR REPLACE VIEW static_view AS
  SELECT 'bar'::TEXT AS col;

CREATE VIEW static_values_view AS
  VALUES('foo'::TEXT);

CREATE OR REPLACE VIEW static_values_view AS
  VALUES('bar'::TEXT);

CREATE VIEW datatype_view AS
  SELECT * FROM datatype_table;

CREATE TEMPORARY VIEW temp_datatype_view AS
  SELECT * FROM datatype_table;

CREATE TEMP TABLE temp_table (
    id INT PRIMARY KEY
);

CREATE VIEW temp_table_view AS
  SELECT * FROM temp_table;

CREATE VIEW person_view
    WITH (security_barrier)
    AS
  SELECT *
    FROM person
   WHERE id > 100
    WITH LOCAL CHECK OPTION;

CREATE RECURSIVE VIEW nums_1_100 (n) AS
    VALUES (1)
UNION ALL
    SELECT n+1 FROM nums_1_100 WHERE n < 100;
