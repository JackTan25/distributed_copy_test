#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

log_command bendsql --dsn "$DSN"  -q \"drop stage if exists st_\"
log_command bendsql --dsn "$DSN"  -q \"drop stage if exists s1_\"
log_command bendsql --dsn "$DSN"  -q \"drop table if exists table_random_\"
log_command bendsql --dsn "$DSN"  -q \"drop table if exists table_random2_\"

log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from t_\" > result1.txt
log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from t_query_\" > result2.txt
log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from products_\" > result3.txt
log_command bendsql --dsn "$DSN"  -q \"drop table if exists products_\"