#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

# log_command bendsql --dsn "$DSN"  -q \"drop stage if exists stage_parquet_\"
# log_command bendsql --dsn "$DSN"  -q \"drop stage if exists stage_parquet_normal_\"

# log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from parquet_table_\" > result1.txt
# log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from parquet_table_normal_\" > result2.txt
