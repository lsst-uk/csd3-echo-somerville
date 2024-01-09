date >>upload_test_upload_checksum_loop_folders_bg_auto_2_cores.log
i=20180906; python upload.py $i >> upload_test_upload_checksum_loop_folders_bg_auto_2_cores.log 2>> upload.err &
a=$!
sleep 1
i=20180907; python upload.py $i >> upload_test_upload_checksum_loop_folders_bg_auto_2_cores.log 2>> upload.err &
b=$!
sleep 1
i=20180911; python upload.py $i >> upload_test_upload_checksum_loop_folders_bg_auto_2_cores.log 2>> upload.err &
c=$!
sleep 1
wait $a; wait $b; wait $c; date >>upload_test_upload_checksum_loop_folders_bg_auto_2_cores.log
