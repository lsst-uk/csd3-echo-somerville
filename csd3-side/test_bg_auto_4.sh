date >>upload_test_upload_checksum_loop_folders_bg_auto_1_4_cores.log
i=20180819; python upload.py $i >> upload_test_upload_checksum_loop_folders_bg_auto_1_4_cores.log 2>> upload.err &
a=$!
sleep 1
i=20180902; python upload.py $i >> upload_test_upload_checksum_loop_folders_bg_auto_1_4_cores.log 2>> upload.err &
b=$!
sleep 1
i=20180905; python upload.py $i >> upload_test_upload_checksum_loop_folders_bg_auto_1_4_cores.log 2>> upload.err &
c=$!
sleep 1
i=20180906; python upload.py $i >> upload_test_upload_checksum_loop_folders_bg_auto_1_4_cores.log 2>> upload.err &
d=$!
wait $a; wait $b; wait $c; wait $d; date >>upload_test_upload_checksum_loop_folders_bg_auto_1_4_cores.log
