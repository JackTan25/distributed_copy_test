#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

log_command bendsql --dsn "$DSN"  -q \"drop stage if exists stage_parquet\"
log_command bendsql --dsn "$DSN"  -q \"drop stage if exists stage_parquet_normal\"
log_command bendsql --dsn "$DSN"  -q \"create table table_random_parquet\(a int,b timestamp,c string\) ENGINE = Random\"
log_command bendsql --dsn "$DSN"  -q \"create table table_random2_parquet\(a int,b timestamp\) ENGINE = Random\"
log_command bendsql --dsn "$DSN"  -q \"create table parquet_table \(a int, b timestamp, c string\) cluster by\(to_yyyymmdd\(b\),a\)\"
log_command bendsql --dsn "$DSN"  -q \"create table parquet_table_normal \(a int, b timestamp, c string\) cluster by\(to_yyyymmdd\(b\),a\)\"
