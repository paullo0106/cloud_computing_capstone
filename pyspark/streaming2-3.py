
'''
usage:
MASTER=spark://{MASTER_URL}.compute-1.amazonaws.com:7077 bin/pyspark streaming2-3.py
'''

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

TOTAL_FILES = 4  # change it in production
STREAMING_INTERVAL = 8
TOP_NUM = 10

MASTER_URL = 'ec2-xx-xx-xx-xx.compute-1.amazonaws.com'
CHECKPOINT_DIR = 'hdfs://{}/user/ubuntu/checkpoint2_3/'.format(MASTER_URL)
TASK_DIR = 'task2_3'

empty_count = 0
dfstream_num = 0


def output_formatter(data):
        return '{},{}, {},{}'.format(data[0][0], data[0][1], data[1][0], data[1][1])


def process(time, rdd):
        print 'process DFStream at {}'.format(str(time))
	print rdd
        print 'RDD count: {}'.format(rdd.count())

        # avoid generating useless output
        if not rdd.isEmpty() and rdd.count() > 0:
                global dfstream_num
                dir_name = 'hdfs://{}/data/tmp/{}/{}'.format(MASTER_URL, TASK_DIR, dfstream_num)
                print 'Inserting RDD {} to {}'.format(dfstream_num, dir_name)
                dfstream_num += 1

                rdd2 = rdd.coalesce(1).map(output_formatter)
        	rdd2.saveAsTextFile(dir_name)

                # and then insert RDD to DynamoDB/Cassandra depending on tasks as requested
                # insertRDDtoDynamoDB(rdd2)
        else:
                global empty_count
                empty_count += 1


def sum_delay(fields):

    carrier = fields[1]
    orig = fields[2]
    dest = fields[3]

    carrier_delay = fields[4].replace('"', '')
    weather_delay = fields[5].replace('"', '')
    nas_delay = fields[6].replace('"', '')
    security_delay = fields[7].replace('"', '')
    late_aircraft_delay = fields[8].replace('"', '')

    #print carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay
    delay_list = [carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay]

    total_delay = 0
    for d in delay_list:
	try:
		total_delay += float(d)
	except:
		continue

    return (orig, dest, carrier), (total_delay, 1)


def updateFunction(newValues, curValue):

	newValue1 = [new[0] for new in newValues]
	newValue2 = [new[1] for new in newValues]
	oldValue1 = curValue[0] if curValue else 0
	oldValue2 = curValue[1] if curValue else 0

	result1 = sum(newValue1, oldValue1)
	result2 = sum(newValue2, oldValue2)

	return (result1, result2)


def createContext():

        conf = SparkConf().setMaster('spark://{}:7077'.format(MASTER_URL)).set('spark.executor.memory', '2g')
        sc = SparkContext(conf=conf)

        ssc = StreamingContext(sc, STREAMING_INTERVAL)
        lines = ssc.textFileStream('hdfs://{}/data/on_time/streaming/'.format(MASTER_URL))

        ssc.checkpoint(CHECKPOINT_DIR)

        # main split-combine-apply logic put here
	# filter out header and other invalid rows
	rdd = lines.map(lambda line: line.split(',')).filter(lambda words: len(words) > 56)
	rdd2 = rdd.map(lambda x: (x[0], x[8], x[11], x[18], x[52], x[53], x[54], x[55], x[56])).map(lambda line: [str(w.replace('"','')) for w in line]).filter(lambda row: row[0] != 'Year' and any(row[4:]))
	rdd2.pprint()

    	# sum up delay fields for each row
	sum_delay_rdd = rdd2.map(sum_delay)
	sum_delay_rdd.pprint()

    	# sum up delay for each (orig, dest, carrier) pair
	combined_rdd = sum_delay_rdd.updateStateByKey(updateFunction)
	combined_rdd.pprint()

    	# calculate avg delay
	avg_rdd = combined_rdd.transform(lambda rdd: rdd.map(lambda (x, y): ((x[0], x[1]), (y[0]/float(y[1]), x[2]))))
	avg_rdd.pprint()

    	# group by (orig, dest)
	avg_rdd_by_route = avg_rdd.groupByKey()

    	# sort by on time performance for each (orig, dest) route and take top 10
	route_sorted_carrier = avg_rdd_by_route.mapValues(lambda x: sorted(list(x))[:10])
	aa = route_sorted_carrier.flatMapValues(lambda x: x)

    	aa.pprint()
	aa.foreachRDD(process)

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

