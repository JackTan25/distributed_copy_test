source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

expected="\"800000\""
for ((i=1; i<=1000; i++)); do
    # 测试 from stage
    log_command bendsql --dsn "$DSN"  -q \"create stage st FILE_FORMAT = \(TYPE = CSV\)\"
    log_command bendsql --dsn "$DSN"  -q \"create stage st_query FILE_FORMAT = \(TYPE = CSV\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @st from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @st from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @st from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @st from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @st from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @st from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @st from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @st from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into t from @st force = true purge = true\"
    res=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from t\")
    last_string=$(echo "$res" | awk -F',' '{print $NF}')
    echo "a $last_string run $i" > ./distributed_copy_test/result2.txt
    if [[ $last_string != $expected ]]; then
        echo "a 发现 mismatch, 结束循环 $last_string2" >> ./distributed_copy_test/result2.txt
        break
    fi
    log_command bendsql --dsn "$DSN"  -q \"truncate table t\"
     # 测试 from query
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into t_query from \(select \\\$1,\\\$2,\\\$3 from @st_query as t2\) force =  true purge = true\"

    res=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from t_query\")
    last_string=$(echo "$res" | awk -F',' '{print $NF}')
    echo "b $last_string run $i" >> ./distributed_copy_test/result2.txt
    if [[ $last_string != $expected ]]; then
        echo "b 发现 mismatch, 结束循环 $last_string2" >> ./distributed_copy_test/result2.txt
        break
    fi
    log_command bendsql --dsn "$DSN"  -q \"truncate table t_query\"

    log_command bendsql --dsn "$DSN"  -q \"drop stage if exists st\"
    log_command bendsql --dsn "$DSN"  -q \"drop stage if exists st_query\"
done
