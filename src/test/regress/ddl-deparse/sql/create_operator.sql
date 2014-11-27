---
--- CREATE_OPERATOR
---



CREATE OPERATOR @#@ (
   rightarg = int8,		-- left unary
   procedure = numeric_fac
);

CREATE OPERATOR #@# (
   leftarg = int8,		-- right unary
   procedure = numeric_fac
);

CREATE OPERATOR #%# (
   leftarg = int8,		-- right unary
   procedure = numeric_fac
);

CREATE OR REPLACE FUNCTION fn_op2(boolean, boolean)
  RETURNS boolean
  LANGUAGE sql
  IMMUTABLE
AS $$
  SELECT NULL::BOOLEAN;
$$;

CREATE OPERATOR === (
    LEFTARG = boolean,
    RIGHTARG = boolean,
    PROCEDURE = fn_op2,
    COMMUTATOR = ===,
    NEGATOR = !==,
    RESTRICT = contsel,
    JOIN = contjoinsel,
    SORT1, SORT2, LTCMP, GTCMP, HASHES, MERGES
);
