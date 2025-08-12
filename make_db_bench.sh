# make clean
DEBUG_LEVEL=0 ROCKSDB_PLUGINS=zenfs make -j64 db_bench install
mv db_bench ./build/db_bench
