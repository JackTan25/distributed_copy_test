source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

expected="\"800000\""
stage_expected="\"0\""
stage_expected2="\"8\""
for ((i=1; i<=1000; i++)); do
    echo "" > ./distributed_copy_test/result2.txt
    # 测试 from stage  
    log_command bendsql --dsn "$DSN"  -q \"create stage st FILE_FORMAT = \(TYPE = CSV\)\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"create stage st_query FILE_FORMAT = \(TYPE = CSV\)\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st from \(select a,b,c from table_random limit 100000\)\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st from \(select a,b,c from table_random limit 100000\)\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st from \(select a,b,c from table_random limit 100000\)\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st from \(select a,b,c from table_random limit 100000\)\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st from \(select a,b,c from table_random limit 100000\)\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st from \(select a,b,c from table_random limit 100000\)\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st from \(select a,b,c from table_random limit 100000\)\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st from \(select a,b,c from table_random limit 100000\)\" >> ./distributed_copy_test/result2.txt

    res1=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from list_stage\(location=\>\'@st\'\)\")
    last_string1=$(echo "$res1" | awk -F',' '{print $NF}')

    echo "start run $i $last_string1" >> ./distributed_copy_test/result2.txt
    if [[ $last_string1 != $stage_expected2 ]]; then
        echo "stage1 build 发现 mismatch, 结束循环 $last_string1" >> ./distributed_copy_test/result2.txt
        break
    fi

    log_command bendsql --dsn "$DSN"  -q \"copy into t from @st force = true purge = true\"
    
    res1=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from list_stage\(location=\>\'@st\'\)\")
    last_string1=$(echo "$res1" | awk -F',' '{print $NF}')

    echo "start run $i $last_string1" >> ./distributed_copy_test/result2.txt
    if [[ $last_string1 != $stage_expected ]]; then
        echo "stage1 发现 mismatch, 结束循环 $last_string1" >> ./distributed_copy_test/result2.txt
        break
    fi

    res=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from t\")
    last_string=$(echo "$res" | awk -F',' '{print $NF}')
    echo "a $last_string run $i" >> ./distributed_copy_test/result2.txt
    if [[ $last_string != $expected ]]; then
        echo "a 发现 mismatch, 结束循环 $last_string2" >> ./distributed_copy_test/result2.txt
        break
    fi
    log_command bendsql --dsn "$DSN"  -q \"truncate table t\" >> ./distributed_copy_test/result2.txt
     # 测试 from query
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query from \(select a,b,c from table_random limit 100000\)\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query from \(select a,b,c from table_random limit 100000\)\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query from \(select a,b,c from table_random limit 100000\)\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query from \(select a,b,c from table_random limit 100000\)\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query from \(select a,b,c from table_random limit 100000\)\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query from \(select a,b,c from table_random limit 100000\)\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query from \(select a,b,c from table_random limit 100000\)\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query from \(select a,b,c from table_random limit 100000\)\" >> ./distributed_copy_test/result2.txt


    res1=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from list_stage\(location=\>\'@st_query\'\)\")
    last_string1=$(echo "$res1" | awk -F',' '{print $NF}')

    echo "start run $i $last_string1" >> ./distributed_copy_test/result2.txt
    if [[ $last_string1 != $stage_expected2 ]]; then
        echo "stage2 build 发现 mismatch, 结束循环 $last_string1" >> ./distributed_copy_test/result2.txt
        break
    fi

    log_command bendsql --dsn "$DSN"  -q \"copy into t_query from \(select \\\$1,\\\$2,\\\$3 from @st_query as t2\) force =  true purge = true\" >> ./distributed_copy_test/result2.txt

    res1=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from list_stage\(location=\>\'@st_query\'\)\")
    last_string1=$(echo "$res1" | awk -F',' '{print $NF}')

    echo "start run $i $last_string1" >> ./distributed_copy_test/result2.txt
    if [[ $last_string1 != $stage_expected ]]; then
        echo "stage2 发现 mismatch, 结束循环 $last_string1" >> ./distributed_copy_test/result2.txt
        break
    fi

    res=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from t_query\")
    last_string=$(echo "$res" | awk -F',' '{print $NF}')
    echo "b $last_string run $i" >> ./distributed_copy_test/result2.txt
    if [[ $last_string != $expected ]]; then
        echo "b 发现 mismatch, 结束循环 $last_string2" >> ./distributed_copy_test/result2.txt
        break
    fi
    log_command bendsql --dsn "$DSN"  -q \"truncate table t_query\" >> ./distributed_copy_test/result2.txt

    log_command bendsql --dsn "$DSN"  -q \"drop stage if exists st\" >> ./distributed_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"drop stage if exists st_query\" >> ./distributed_copy_test/result2.txt
done
