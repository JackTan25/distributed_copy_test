#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

log_command bendsql --dsn "$DSN"  -q \"drop stage if exists stage_parquet\"
log_command bendsql --dsn "$DSN"  -q \"drop stage if exists stage_parquet_normal\"
# res1=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from parquet_table\")
# last_string1=$(echo "$res1" | awk -F',' '{print $NF}')
# echo "$last_string1" > ./distributed_copy_parquet/result1.txt
# res2=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from parquet_table_normal\")
# last_string2=$(echo "$res2" | awk -F',' '{print $NF}')
# echo "$last_string2" > ./distributed_copy_parquet/result2.txt
