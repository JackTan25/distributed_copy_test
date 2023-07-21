#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

log_command bendsql --dsn "$DSN"  -q \"drop stage if exists st\"
log_command bendsql --dsn "$DSN"  -q \"drop stage if exists st_query\"
log_command bendsql --dsn "$DSN"  -q \"drop stage if exists s1\"
log_command bendsql --dsn "$DSN"  -q \"create table table_random\(a int,b string,c string\) ENGINE = Random\"
log_command bendsql --dsn "$DSN"  -q \"create table table_random2\(a int,b string\) ENGINE = Random\"
log_command bendsql --dsn "$DSN"  -q \"create table products \(id int, name string, description string\)\"
log_command bendsql --dsn "$DSN"  -q \"create table t\(a int,b string,c string\)\"
log_command bendsql --dsn "$DSN"  -q \"create table t_query\(a int,b string,c string\)\"