# Cloud Computing Capstone
Placeholder for my scripts in MOOC final project

Hadoop and MapReduce part

![System Architecture](/system_architecture.png?raw=true "")

Spark part

![Architecture](/Spark_Architecture.png?raw=true "")

![Spark Streaming](/Spark_Streaming.png?raw=true "")

![RDD pipeline 1](/Figure1.png?raw=true "")

![RDD pipeline 2](/Figure2.png?raw=true "")

### Prerequisite

* AWS Python SDK
```
pip install boto3
pip install awscli
```

* [mrjob](http://github.com/Yelp/mrjob/) as MapReduce framework in Python
```
pip install mrjob
```

* Download [Transportation dataset](https://aws.amazon.com/datasets/transportation-databases/)

* Run **DynamoDB**

* Set up a **Hadoop cluster** (or run in single computer)

### Example

Execute the python scripts on master NameNode which dispatches MapReduce jobs to its Hadoop cluster,
the result will be saved in HDFS (and DynamoDB)
```
python mr_job_capstone2-1.py -r hadoop hdfs://<namenode address>:/data/orig_dest/*.csv -o hdfs://<namenode address>:/data/q2_1_output/
python dynamodb-crud/query_table2-1.py <airport name>
```

### Data

```
ubuntu@ec2-52-xxx-xxx-253:~$ hadoop fs -dus /data/*
hdfs://ec2-52-xxx-xxx-52.compute-1.amazonaws.com:8020/data/on_time    30547789179
hdfs://ec2-52-xxx-xxx-52.compute-1.amazonaws.com:8020/data/orig_dest    32576029024

ubuntu@ec2-52-xxx-xxx-253:~$ hadoop fs -ls /data/on_time
Found 194 items
-rw-r--r--   2 ubuntu supergroup  142196587 2016-02-06 05:23 /data/on_time/On_Time_On_Time_Performance_1990_1.csv
-rw-r--r--   2 ubuntu supergroup  131244457 2016-02-06 05:23 /data/on_time/On_Time_On_Time_Performance_1990_10.csv
-rw-r--r--   2 ubuntu supergroup  123724241 2016-02-06 05:23 /data/on_time/On_Time_On_Time_Performance_1990_11.csv
-rw-r--r--   2 ubuntu supergroup  125724649 2016-02-06 05:23 /data/on_time/On_Time_On_Time_Performance_1990_12.csv
-rw-r--r--   2 ubuntu supergroup  112418169 2016-02-06 05:23 /data/on_time/On_Time_On_Time_Performance_1990_2.csv
-rw-r--r--   2 ubuntu supergroup  127615271 2016-02-06 05:23 /data/on_time/On_Time_On_Time_Performance_1990_3.csv
...

ubuntu@ec2-52-xxx-xxx-253:~$ hadoop fs -ls /data/orig_dest
Found 22 items
-rw-r--r--   2 ubuntu supergroup 1201493617 2016-02-05 17:37 /data/orig_dest/Origin_and_Destination_Survey_DB1BCoupon_2003_1.csv
-rw-r--r--   2 ubuntu supergroup 1424484646 2016-02-05 18:10 /data/orig_dest/Origin_and_Destination_Survey_DB1BCoupon_2003_2.csv
-rw-r--r--   2 ubuntu supergroup 1356612538 2016-02-05 17:37 /data/orig_dest/Origin_and_Destination_Survey_DB1BCoupon_2003_3.csv
-rw-r--r--   2 ubuntu supergroup 1356612538 2016-02-05 17:37 /data/orig_dest/Origin_and_Destination_Survey_DB1BCoupon_2003_4.csv
...
```