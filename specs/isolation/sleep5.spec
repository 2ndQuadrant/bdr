conninfo "node1" "dbname=node1"
conninfo "node2" "dbname=node2"
conninfo "node3" "dbname=node3"

session "snode1"
step "s1sleep" { SELECT pg_sleep(5); }

session "snode2"
step "s2sleep" { SELECT pg_sleep(5); }

session "snode3"
step "s3sleep" { SELECT pg_sleep(5); }

permutation "s1sleep" "s2sleep" "s3sleep"
