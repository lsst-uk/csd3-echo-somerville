date >>upload_test_upload_checksum_loop_folders_bg_auto_1_6_cores.log
i=20180819; python upload.py $i >> upload_test_upload_checksum_loop_folders_bg_auto_1_6_cores.log 2>> upload.err &
a=$!
wait $a; date >>upload_test_upload_checksum_loop_folders_bg_auto_1_6_cores.log
