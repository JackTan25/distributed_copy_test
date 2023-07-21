source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

expected="\"800000\""
for ((i=1; i<=1000; i++)); do
    # 测试 from stage
    log_command bendsql --dsn "$DSN"  -q \"create stage stage_parquet_normal_ FILE_FORMAT = \(TYPE = PARQUET\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet_normal_ from \(select a,b,c from table_random_parquet_ limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet_normal_ from \(select a,b,c from table_random_parquet_ limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet_normal_ from \(select a,b,c from table_random_parquet_ limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet_normal_ from \(select a,b,c from table_random_parquet_ limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet_normal_ from \(select a,b,c from table_random_parquet_ limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet_normal_ from \(select a,b,c from table_random_parquet_ limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet_normal_ from \(select a,b,c from table_random_parquet_ limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @stage_parquet_normal_ from \(select a,b,c from table_random_parquet_ limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into parquet_table_normal_ from @stage_parquet_normal_ force = true\"
    log_command bendsql --dsn "$DSN"  -q \"drop stage if exists stage_parquet_normal_\"
    res1=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from parquet_table_normal_\")
    last_string1=$(echo "$res1" | awk -F',' '{print $NF}')

    echo "start run $i $last_string1" > ./standalone_copy_parquet/result2.txt
    if [[ $last_string1 != $expected ]]; then
        echo "发现 mismatch, 结束循环 $last_string1" >> ./standalone_copy_parquet/result2.txt
        break
    fi
    log_command bendsql --dsn "$DSN"  -q \"truncate table parquet_table_normal_\"
done