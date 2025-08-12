#!/bin/bash

echo "========== RocksDB with ZNS SSD =========="
echo ""

if [ ! $1 ] 
then
	echo "$0 workload_type (1: random, 2:sequential, 3:zippyDB)"
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
rm -r /data2/doeun/zns_zenfs_aux01/
blkzone reset /dev/nvme3n2
max_bk_jobs=$2
num=150000000
echo "thread $max_bk_jobs !!!!!!!!!!"

../plugin/zenfs/util/zenfs mkfs --zbd=/nvme3n2 --aux_path=/data2/doeun/zns_zenfs_aux01/ --enable_gc=true --force

echo "Complete!"

fi

sleep 1
 
if [ $1 -eq 1 ]
then	
	echo "Random Write"
#./db_bench --fs_uri=zenfs://dev:nvme3n2 --benchmarks=fillrandom,stats --use_direct_io_for_flush_and_compaction --compression_type=none --value_size=1024 --num=100000000 --max_background_jobs=8 #--stats_interval_seconds=1
	./db_bench --fs_uri=zenfs://dev:nvme3n2 --benchmarks=fillrandom,stats --use_direct_io_for_flush_and_compaction --compression_type=none --value_size=1000 --num=300000000 --max_background_jobs=$max_bk_jobs  #--statistics=true
#--stats_interval_seconds=1
elif [ $1 -eq 2 ]
then
	echo "Overwrite"
	./db_bench_type1_hit_and_zones --fs_uri=zenfs://dev:nvme3n2 --benchmarks=overwrite,stats --use_direct_io_for_flush_and_compaction --compression_type=none --value_size=1024 --num=100000000 --max_background_jobs=$max_bk_jobs 
elif [ $1 -eq 3 ] 
then
	echo "ZippyDB"
	./db_bench --fs_uri=zenfs://dev:nvme3n2 --benchmarks=mixgraph,stats --use_direct_io_for_flush_and_compaction --compression_type=none --keyrange_dist_a=14.18 -keyrange_dist_b=-2.917 -keyrange_dist_c=0.0164 -keyrange_dist_d=-0.08082 -keyrange_num=30 --value_k=0.2615 -value_sigma=25.45 -mix_get_ratio=0 -mix_put_ratio=1 --num=500000000 -key_size=48 -sine_mix_rate_interval_milliseconds=5000 -sine_a=1000 -sine_b=0.000073 -sine_d=4500 --max_background_jobs=$max_bk_jobs
elif [ $1 -eq 4 ]
then
	echo "Creating base data for Facebook workloads"
	./db_bench --fs_uri=zenfs://dev:nvme3n2 --benchmarks=fillrandom --use_direct_io_for_flush_and_compaction --compression_type=none -cache_size=268435456 -key_size=48 -value_size=43 -num=50000000
elif [ $1 -eq 5 ] #<-------------------------
then
	echo "420K 300M FillRandom + Overwrite"
	./db_bench --fs_uri=zenfs://dev:nvme3n2 --benchmarks=fillrandom,overwrite,stats --use_direct_io_for_flush_and_compaction \
		--key_size=20 --value_size=800 --num=$num --max_background_jobs=$max_bk_jobs --disable_wal
elif [ $1 -eq 6 ]
then
	echo "GDB 16"
	gdb --args ./db_bench --fs_uri=zenfs://dev:nvme3n2 --benchmarks=fillrandom,overwrite,stats --use_direct_io_for_flush_and_compaction \
	--key_size=20 --value_size=800 --num=80000000 --max_background_jobs=16 --disable_wal
elif [ $1 -eq 7 ]
then
	echo "420K 3M FillRandom + Overwrite"
	./db_bench --fs_uri=zenfs://dev:nvme3n2 --benchmarks=fillrandom,overwrite,stats --use_direct_io_for_flush_and_compaction \
		--key_size=20 --value_size=800 --num=$num --max_background_jobs=$max_bk_jobs --disable_wal 1> /data2/doeun/tmpfs/jinyoung/caza_gc_util_v2 2>&1
fi