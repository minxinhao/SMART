python3 ../ycsb/split_workload.py a randint 1 1
make -j
/bin/bash ../script/restartMemc.sh
./ycsb_test 1 1 1 randint a fix_range_size