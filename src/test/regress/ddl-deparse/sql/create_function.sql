CREATE FUNCTION check_foreign_key ()
	RETURNS trigger
	AS '/Users/barwick/devel/postgres/src/2ndquadrant_bdr/src/test/regress/ddl-deparse/refint.so'
	LANGUAGE C;