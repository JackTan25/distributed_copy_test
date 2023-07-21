#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

log_command bendsql --dsn "$DSN"  -q \"drop stage if exists stage_parquet_\"
log_command bendsql --dsn "$DSN"  -q \"drop stage if exists stage_parquet_normal_\"
log_command bendsql --dsn "$DSN"  -q \"create table table_random_parquet_\(a int,b timestamp,c string\) ENGINE = Random\"
log_command bendsql --dsn "$DSN"  -q \"create table table_random2_parquet_\(a int,b timestamp\) ENGINE = Random\"
log_command bendsql --dsn "$DSN"  -q \"create table parquet_table_ \(a int, b timestamp, c string\) cluster by\(to_yyyymmdd\(b\),a\)\"
log_command bendsql --dsn "$DSN"  -q \"create table parquet_table_normal_ \(a int, b timestamp, c string\) cluster by\(to_yyyymmdd\(b\),a\)\"
