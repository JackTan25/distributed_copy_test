#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

log_command bendsql --dsn "$DSN"  -q \"drop stage if exists st_\"
log_command bendsql --dsn "$DSN"  -q \"drop stage if exists st_query_\"
log_command bendsql --dsn "$DSN"  -q \"drop stage if exists s1_\"
log_command bendsql --dsn "$DSN"  -q \"create table table_random_\(a int,b string,c string\) ENGINE = Random\"
log_command bendsql --dsn "$DSN"  -q \"create table table_random2_\(a int,b string\) ENGINE = Random\"
log_command bendsql --dsn "$DSN"  -q \"create table products_ \(id int, name string, description string\)\"
log_command bendsql --dsn "$DSN"  -q \"create table t_\(a int,b string,c string\)\"
log_command bendsql --dsn "$DSN"  -q \"create table t_query_\(a int,b string,c string\)\"