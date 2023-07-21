source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

expected="\"0\""
for i in {1..1000}
do
    # 测试 from stage
    log_command bendsql --dsn "$DSN"  -q \"create stage stage_parquet FILE_FORMAT = \(TYPE = PARQUET\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet from \(select a,b,c from table_random_parquet limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet from \(select a,b,c from table_random_parquet limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet from \(select a,b,c from table_random_parquet limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet from \(select a,b,c from table_random_parquet limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet from \(select a,b,c from table_random_parquet limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet from \(select a,b,c from table_random_parquet limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet from \(select a,b,c from table_random_parquet limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet from \(select a,b from table_random2_parquet limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into parquet_table from @stage_parquet force = true\"
    res1=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from parquet_table\")
    last_string1=$(echo "$res1" | awk -F',' '{print $NF}')

    echo "start run $i $last_string1" > ./distributed_copy_parquet/result1.txt
    if [[ $last_string1 != $expected ]]; then
        echo "发现 mismatch, 结束循环 $last_string1" >> ./distributed_copy_parquet/result1.txt
        break
    fi
    log_command bendsql --dsn "$DSN"  -q \"drop stage if exists stage_parquet\"
done
