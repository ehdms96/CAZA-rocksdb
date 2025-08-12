#!/bin/bash

echo "========== RocksDB with ZNS SSD =========="
echo ""

if [ ! $1 ] 
then
	echo "$0 workload_type (0: load, 1: A, 2: B, 3: C, 4: D, 5: E, 6: F)"
	exit 1 
fi

if [ ! $2 ] 
then
	echo "$0 Thread num (2 or 4 or 8)"
	exit 1 
fi

if [ $1 -ne 8 ]
then

echo ""
echo "Init filesystem (CAZA)"
rm -r /data2/doeun/zns_zenfs_aux02/
blkzone reset /dev/nvme3n2
max_bk_jobs=$2

../plugin/zenfs/util/zenfs mkfs --zbd=/nvme3n2 --aux_path=/data2/doeun/zns_zenfs_aux02/ --enable_gc=true --force

echo "Complete!"

fi

num_record=150000000
num_op=$(expr $num_record / $3) # $ is io-thread, static = 128

echo "max_background_jobs $max_bk_jobs !!!!!!!!!! num_record $4 !!!!!!!!!! io_thread $3 !!!!!!!!!! num_op $num_op !!!!!!!!!!"

sleep 1

if [ $1 -eq 0 ]
then	
	echo "ycsb load"
elif [ $1 -eq 1 ]
then
	echo "LOAD & WOARLOAD A START"
	echo "num_op : $num_op"
	gdb --args ./db_bench --fs_uri=zenfs://dev:nvme3n2 --benchmarks=YCSBLOAD,YCSBA,stats --use_direct_io_for_flush_and_compaction \
				--compression_type=none --num_record=$num_record --num_op=$num_op --max_background_jobs=$2 --threads=$3 --disable_wal \
				1> /data2/doeun/tmpfs/jinyoung/caza_A_test 2>&1
elif [ $1 -eq 2 ] 
then
	echo "LOAD & WOARLOAD B START"
	echo "num_op : $num_op"
	gdb --args ./db_bench --fs_uri=zenfs://dev:nvme3n2 --benchmarks=YCSBLOAD,YCSBB,stats --use_direct_io_for_flush_and_compaction \
				--compression_type=none --num_record=$num_record --num_op=$num_op --max_background_jobs=$2 --threads=$3 --disable_wal \
				1> /data2/doeun/tmpfs/jinyoung/caza_T8_B2 2>&1
elif [ $1 -eq 3 ]
then
	echo "LOAD & WOARLOAD A START"
	echo "num_op : $num_op"
	gdb --args ./db_bench --fs_uri=zenfs://dev:nvme3n2 --benchmarks=YCSBLOAD,YCSBA,stats --use_direct_io_for_flush_and_compaction \
				--compression_type=none --num_record=$num_record --num_op=$num_op --max_background_jobs=$2 --threads=$3 --disable_wal #> /data2/doeun/tmpfs/doeun/type2_CAZA/ycsb/type2_YCSB_A_T$2_S90_S3_EX.txt 2>&1
elif [ $1 -eq 4 ] 
then
	echo "LOAD & WOARLOAD B START" 
	echo "num_op : $num_op"
	gdb --args ./db_bench --fs_uri=zenfs://dev:nvme3n2 --benchmarks=YCSBLOAD,YCSBB,stats --use_direct_io_for_flush_and_compaction \
				--compression_type=none --num_record=$num_record --num_op=$num_op --max_background_jobs=$2 --threads=$3 --disable_wal #> /data2/doeun/tmpfs/doeun/type2_CAZA/ycsb/type2_YCSB_B_T$2_S90_S3_EX.txt 2>&1
fi
