# ./caza_test.sh 5 8 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T8_0.txt 2>&1
# ./caza_test.sh 5 12 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T12_1.txt 2>&1
# ./caza_test.sh 5 16 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T16_1.txt 2>&1

# echo "caza 12, 16 done" | mail -s "ycsb 16th" doeun96@naver.com -aFrom:doeun96@splab
./caza_test.sh 5 8 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T8_2.txt 2>&1
./caza_test.sh 5 12 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T12_2.txt 2>&1
./caza_test.sh 5 16 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T16_2.txt 2>&1


./caza_test.sh 5 8 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T8_3.txt 2>&1
./caza_test.sh 5 12 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T12_3.txt 2>&1
./caza_test.sh 5 16 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T16_3.txt 2>&1

./caza_test.sh 5 8 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T8_4.txt 2>&1
./caza_test.sh 5 12 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T12_4.txt 2>&1
./caza_test.sh 5 16 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T16_4.txt 2>&1

./caza_test.sh 5 8 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T8_5.txt 2>&1
./caza_test.sh 5 12 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T12_5.txt 2>&1
./caza_test.sh 5 16 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T16_5.txt 2>&1

./caza_test.sh 5 4 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T4_1.txt 2>&1
./caza_test.sh 5 2 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T2_1.txt 2>&1
./caza_test.sh 5 4 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T4_2.txt 2>&1
./caza_test.sh 5 2 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T2_2.txt 2>&1
./caza_test.sh 5 4 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T4_3.txt 2>&1
./caza_test.sh 5 2 280000000 > /data2/doeun/tmpfs/Revise/doeun/caza/caza_dbb_T2_3.txt 2>&1

echo "caza all done" | mail -s "ycsb 16th" doeun96@naver.com -aFrom:doeun96@splab

# ./caza_ycsb_test.sh 4 8 128 150000000 > /data2/doeun/tmpfs/doeun/type2_CAZA/ycsb/type2_YCSB_B_T8_S90_S3_EX.txt 2>&1
# echo "B T8 done!!"

# ./caza_ycsb_test.sh 4 4 128 150000000 > /data2/doeun/tmpfs/doeun/type2_CAZA/ycsb/type2_YCSB_B_T4_S90_S3_EX.txt 2>&1
# echo "B T4 done!!"

# ./caza_ycsb_test.sh 4 2 128 150000000 > /data2/doeun/tmpfs/doeun/type2_CAZA/ycsb/type2_YCSB_B_T2_S90_S3_EX.txt 2>&1
# echo "B T2 done!!"

# ./caza_test.sh 5 8 > /data2/doeun/tmpfs/doeun/type2_T8_S90_S3_GCX_EX.txt 2>&1
# echo "./type2_T8_S90_S3_EX done!!"

# ./caza_test.sh 5 4 > /data2/doeun/tmpfs/doeun/type2_T4_S90_S3_GCX_EX.txt 2>&1
# echo "./type2_T4_S90_S3_EX done!!"

# ./caza_test.sh 5 2 > /data2/doeun/tmpfs/doeun/type2_T2_S90_S3_GCX_EX.txt 2>&1
# echo "./type2_T2_S90_S3_EX done!!"

# ./caza_test.sh 5 8 > /data2/doeun/tmpfs/doeun/type2_T8_S90_S3_EX_STDOUT.txt 2> /data2/doeun/tmpfs/doeun/type2_T8_S90_S3_EX_STDERR.txt
# echo "./type2_T8_S90_S3_EX done!!"

# ./caza_test.sh 5 4 > /data2/doeun/tmpfs/doeun/type2_T4_S90_S3_EX_STDOUT.txt 2> /data2/doeun/tmpfs/doeun/type2_T4_S90_S3_EX_STDERR.txt
# echo "./type2_T4_S90_S3_EX done!!"

# ./caza_test.sh 5 2 > /data2/doeun/tmpfs/doeun/type2_T2_S90_S3_EX_STDOUT.txt 2> /data2/doeun/tmpfs/doeun/type2_T2_S90_S3_EX_STDERR.txt
# echo "./type2_T2_S90_S3_EX done!!"