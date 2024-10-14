find . -name "tree_output_*_*_*.txt" -delete
python3 ../ycsb/split_workload.py a randint 1 2
make -j
/bin/bash ../script/restartMemc.sh
./ycsb_test 1 2 2 randint a fix_range_size