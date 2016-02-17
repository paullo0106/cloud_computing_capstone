
'''
 MASTER=spark://{MASTER_URL}.compute-1.amazonaws.com:7077 bin/pyspark streaming1-2.py
'''

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

TOTAL_FILES = 12
STREAMING_INTERVAL = 8

MASTER_URL = 'ec2-xx-xx-xx-xx.compute-1.amazonaws.com'
CHECKPOINT_DIR = 'hdfs://{}/user/ubuntu/checkpoint/'.format(MASTER_URL)
TASK_DIR = 'word_count'

empty_count = 0
dfstream_num = 0


def process(time, rdd):
        print 'process DFStream at {}'.format(str(time))
        print 'RDD count: {}'.format(rdd.count())

        # avoid generating useless output
        if not rdd.isEmpty() and rdd.count() > 0:
                global dfstream_num
                dir_name = 'hdfs://{}/data/tmp/{}/{}'.format(MASTER_URL, TASK_DIR, dfstream_num)
                print 'Inserting RDD {} to {}'.format(dfstream_num, dir_name)
                dfstream_num += 1

                rdd2 = rdd.coalesce(1)
                rdd2.saveAsHadoopFile(dir_name, 'org.apache.hadoop.mapred.TextOutputFormat')
        else:
                global empty_count
                empty_count += 1


def updateFunction(newValues, runningCount):
        if runningCount is None:
                runningCount = 0
        return sum(newValues, runningCount)


def createContext():

        conf = SparkConf().setMaster('spark://{}:7077'.format(MASTER_URL)).set('spark.executor.memory', '2g')
        sc = SparkContext(conf=conf)

        ssc = StreamingContext(sc, STREAMING_INTERVAL)
        lines = ssc.textFileStream('hdfs://{}/data/on_time/streaming/'.format(MASTER_URL))

        ssc.checkpoint(CHECKPOINT_DIR)

        # main split-combine-apply logic put here
        pairs = lines.map(lambda x: x.split(",")).map(lambda x: (x[8], 1))
        runningCounts = pairs.updateStateByKey(updateFunction)

        sortedCounts = runningCounts.transform(lambda rdd: rdd.sortBy(lambda (airport, freq): freq, ascending=False))
        sortedCounts.foreachRDD(process)

        return ssc


ssc = StreamingContext.getOrCreate(CHECKPOINT_DIR, createContext)
ssc.start()
finish_delay = 0
while True:
    res = ssc.awaitTerminationOrTimeout(2)
    if (dfstream_num >= TOTAL_FILES):
        finish_delay += 1

    # stop logic
    if res or empty_count >= 10 or finish_delay >= 10:
        # stopped elsewhere
        print 'Finished processing all {} files, terminate the streaming job!!!!!!'.format(TOTAL_FILES)
        ssc.stop(stopSparkContext=True, stopGraceFully=True)
        break


print 'FINISHED..................'