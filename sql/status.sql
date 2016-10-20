SELECT
  c::"char" AS status_char,
  bdr.node_status_from_char(c::"char") AS status_str,
  bdr.node_status_to_char(bdr.node_status_from_char(c::"char")) AS roundtrip_char
FROM (VALUES ('b'),('i'),('c'),('o'),('r'),('k')) x(c)
ORDER BY c;
