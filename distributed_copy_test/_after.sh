#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

log_command bendsql --dsn "$DSN"  -q \"drop stage if exists st\"
log_command bendsql --dsn "$DSN"  -q \"drop stage if exists s1\"
log_command bendsql --dsn "$DSN"  -q \"drop table if exists table_random\"
log_command bendsql --dsn "$DSN"  -q \"drop table if exists table_random2\"
log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from t\" > result1.txt
log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from t_query\" > result2.txt
log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from products\" > result3.txt
log_command bendsql --dsn "$DSN"  -q \"drop table if exists products\"