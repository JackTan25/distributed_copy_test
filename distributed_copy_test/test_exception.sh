source "$(dirname "${BASH_SOURCE[0]}")/../utils/logging.sh"
DSN=$DSN

expected="\"0\""
expected2="\"800000\""
stage_expected="\"8\""
stage_expected2="\"0\""
for ((i=1; i<=1000; i++)); do
    # 测试 from stage
    log_command bendsql --dsn "$DSN"  -q \"create stage s1 FILE_FORMAT = \(TYPE = CSV\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @s1 from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @s1 from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @s1 from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @s1 from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @s1 from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @s1 from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @s1 from \(select a,b,c from table_random limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into @s1 from \(select a,b from table_random2 limit 100000\)\"
    log_command bendsql --dsn "$DSN"  -q \"copy into products from @s1 force = true purge = true\"

    res1=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from list_stage\(location=\>\'@s1\'\)\")
    last_string1=$(echo "$res1" | awk -F',' '{print $NF}')

    echo "start run $i $last_string1" > ./distributed_copy_test/result1.txt
    if [[ $last_string1 != $stage_expected ]]; then
        echo "stage 发现 mismatch, 结束循环 $last_string1" >> ./distributed_copy_test/result1.txt
        break
    fi


    res1=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from products\")
    last_string1=$(echo "$res1" | awk -F',' '{print $NF}')

    echo "start run $i $last_string1" >>./distributed_copy_test/result1.txt
    if [[ $last_string1 != $expected ]]; then
        echo "a 发现 mismatch, 结束循环 $last_string1" >> ./distributed_copy_test/result1.txt
        break
    fi
     # 测试 from query
    log_command bendsql --dsn "$DSN"  -q \"copy into products from \(select \\\$1,\\\$2,\\\$4 from @s1 as t2\) force = true purge=true\"
    
    res1=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from list_stage\(location=\>\'@s1\'\)\")
    last_string1=$(echo "$res1" | awk -F',' '{print $NF}')

    echo "start run $i $last_string1" >> ./distributed_copy_test/result1.txt
    if [[ $last_string1 != $stage_expected2 ]]; then
        echo "stage2 发现 mismatch, 结束循环 $last_string1" >> ./distributed_copy_test/result1.txt
        break
    fi

    

    res1=$(log_command bendsql --dsn "$DSN"  -q \"select count\(\*\) from products\")
    last_string1=$(echo "$res1" | awk -F',' '{print $NF}')

    echo "start run $i $last_string1" >> ./distributed_copy_test/result1.txt
    if [[ $last_string1 != $expected2 ]]; then
        echo "b 发现 mismatch, 结束循环 $last_string1" >> ./distributed_copy_test/result1.txt
        break
    fi
    log_command bendsql --dsn "$DSN"  -q \"truncate table products\"
    log_command bendsql --dsn "$DSN"  -q \"drop stage if exists s1\"
done
