#!/bin/bash

for VARIABLE in $(ls -ltra ~/hadoop_data/on_time | awk '{print $9}'| grep csv | grep 1992)
do
	ls -ltr ~/hadoop_data/on_time/$VARIABLE
	FILE_PATH=~/hadoop_data/on_time/$VARIABLE 
	echo "Copying $FILE_PATH to /data/on_time/streaming"
	hadoop fs -put $FILE_PATH /data/on_time/streaming/
	echo "Finished copying $FILE_PATH"
	sleep 5
done

