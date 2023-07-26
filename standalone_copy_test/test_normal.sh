source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

expected="\"800000\""
stage_expected="\"0\""
stage_expected2="\"8\""
for ((i=1; i<=1000; i++)); do
    echo "" > ./standalone_copy_test/result2.txt
    # 测试 from stage
    log_command bendsql --dsn "$DSN"  -q \"create stage st_ FILE_FORMAT = \(TYPE = CSV\)\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"create stage st_query_ FILE_FORMAT = \(TYPE = CSV\)\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_ from \(select a,b,c from table_random_ limit 100000\)\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_ from \(select a,b,c from table_random_ limit 100000\)\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_ from \(select a,b,c from table_random_ limit 100000\)\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_ from \(select a,b,c from table_random_ limit 100000\)\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_ from \(select a,b,c from table_random_ limit 100000\)\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_ from \(select a,b,c from table_random_ limit 100000\)\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_ from \(select a,b,c from table_random_ limit 100000\)\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_ from \(select a,b,c from table_random_ limit 100000\)\" >> ./standalone_copy_test/result2.txt
    
    res1=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from list_stage\(location=\>\'@st_\'\)\")
    last_string1=$(echo "$res1" | awk -F',' '{print $NF}')

    echo "start run $i $last_string1" >> ./standalone_copy_test/result2.txt
    if [[ $last_string1 != $stage_expected2 ]]; then
        echo "stage1 build 发现 mismatch, 结束循环 $last_string1" >> ./standalone_copy_test/result2.txt
        break
    fi
    
    log_command bendsql --dsn "$DSN"  -q \"copy into t_ from @st_ force = true purge = true\" >> ./standalone_copy_test/result2.txt

    res1=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from list_stage\(location=\>\'@st_\'\)\")
    last_string1=$(echo "$res1" | awk -F',' '{print $NF}')

    echo "start run $i $last_string1" >> ./standalone_copy_test/result2.txt
    if [[ $last_string1 != $stage_expected ]]; then
        echo "stage1 发现 mismatch, 结束循环 $last_string1" >> ./standalone_copy_test/result2.txt
        break
    fi

    res=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from t_\")
    last_string=$(echo "$res" | awk -F',' '{print $NF}')
    echo "a $last_string run $i" >> ./standalone_copy_test/result2.txt
    if [[ $last_string != $expected ]]; then
        echo "a 发现 mismatch, 结束循环 $last_string2" >> ./standalone_copy_test/result2.txt
        break
    fi
    log_command bendsql --dsn "$DSN"  -q \"truncate table t_\" >> ./standalone_copy_test/result2.txt
     # 测试 from query
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query_ from \(select a,b,c from table_random_ limit 100000\)\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query_ from \(select a,b,c from table_random_ limit 100000\)\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query_ from \(select a,b,c from table_random_ limit 100000\)\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query_ from \(select a,b,c from table_random_ limit 100000\)\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query_ from \(select a,b,c from table_random_ limit 100000\)\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query_ from \(select a,b,c from table_random_ limit 100000\)\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query_ from \(select a,b,c from table_random_ limit 100000\)\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"copy into @st_query_ from \(select a,b,c from table_random_ limit 100000\)\" >> ./standalone_copy_test/result2.txt

    res1=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from list_stage\(location=\>\'@st_query_\'\)\")
    last_string1=$(echo "$res1" | awk -F',' '{print $NF}')

    echo "start run $i $last_string1" >> ./standalone_copy_test/result2.txt
    if [[ $last_string1 != $stage_expected2 ]]; then
        echo "stage2 build 发现 mismatch, 结束循环 $last_string1" >> ./standalone_copy_test/result2.txt
        break
    fi

    log_command bendsql --dsn "$DSN"  -q \"copy into t_query_ from \(select \\\$1,\\\$2,\\\$3 from @st_query_ as t2\) force =  true purge = true\" >> ./standalone_copy_test/result2.txt

    res1=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from list_stage\(location=\>\'@st_query_\'\)\")
    last_string1=$(echo "$res1" | awk -F',' '{print $NF}')

    echo "start run $i $last_string1" >> ./standalone_copy_test/result2.txt
    if [[ $last_string1 != $stage_expected ]]; then
        echo "stage2 发现 mismatch, 结束循环 $last_string1" >> ./standalone_copy_test/result2.txt
        break
    fi


    res=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from t_query_\")
    last_string=$(echo "$res" | awk -F',' '{print $NF}')
    echo "b $last_string run $i" >> ./standalone_copy_test/result2.txt
    if [[ $last_string != $expected ]]; then
        echo "b 发现 mismatch, 结束循环 $last_string2" >> ./standalone_copy_test/result2.txt
        break
    fi
    log_command bendsql --dsn "$DSN"  -q \"truncate table t_query_\" >> ./standalone_copy_test/result2.txt

    log_command bendsql --dsn "$DSN"  -q \"drop stage if exists st_\" >> ./standalone_copy_test/result2.txt
    log_command bendsql --dsn "$DSN"  -q \"drop stage if exists st_query_\" >> ./standalone_copy_test/result2.txt
done
