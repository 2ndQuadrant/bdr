---
--- CREATE_TRIGGER
---

CREATE TRIGGER trigger_1
  BEFORE INSERT OR UPDATE
  ON datatype_table
  FOR EACH ROW
  EXECUTE PROCEDURE plpgsql_function_trigger_1();

CREATE TRIGGER trigger_2
  AFTER DELETE
  ON datatype_table
  FOR EACH ROW
  EXECUTE PROCEDURE plpgsql_function_trigger_2();

CREATE TRIGGER trigger_3
  AFTER TRUNCATE
  ON datatype_table
  FOR EACH STATEMENT
  EXECUTE PROCEDURE plpgsql_function_trigger_2();

CREATE TRIGGER trigger_4
  BEFORE INSERT OR UPDATE
  ON datatype_table
  FOR EACH ROW
  WHEN ( NEW.id > 100 )
  EXECUTE PROCEDURE plpgsql_function_trigger_1();
