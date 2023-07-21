source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

expected="\"800000\""
for i in {1..1000}
do
    # 测试 from stage
    log_command bendsql --dsn "$DSN"  -q \"create stage stage_parquet_normal FILE_FORMAT = \(TYPE = PARQUET\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet_normal from \(select a,b,c from table_random_parquet limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet_normal from \(select a,b,c from table_random_parquet limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet_normal from \(select a,b,c from table_random_parquet limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet_normal from \(select a,b,c from table_random_parquet limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet_normal from \(select a,b,c from table_random_parquet limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet_normal from \(select a,b,c from table_random_parquet limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet_normal from \(select a,b,c from table_random_parquet limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet_normal from \(select a,b,c from table_random_parquet limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into parquet_table_normal from @stage_parquet_normal force = true\"
    res2=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from parquet_table_normal\")
    last_string2=$(echo "$res2" | awk -F',' '{print $NF}')
    echo "start run $i $last_string2" > ./distributed_copy_parquet/result2.txt
    if [[ $last_string2 != $expected ]]; then
        echo "发现 mismatch, 结束循环 $last_string2" >> ./distributed_copy_parquet/result2.txt
        break
    fi
    log_command bendsql --dsn "$DSN"  -q \"truncate table parquet_table_normal\"
    log_command bendsql --dsn "$DSN"  -q \"drop stage if exists stage_parquet_normal\"
done