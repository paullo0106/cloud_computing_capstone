
#python mr_job_capstone3-2.py mr_job_capstone3-2.py -r hadoop hdfs://<url>:/data/on_time/On_Time_On_Time_Performance_2005_10.csv -o hdfs://<url>:/data/q3_2_output10

from mrjob.job import MRJob
from mrjob.step import MRStep
from my_credential import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DYNAMODB_ENDPOINT_URL
import datetime
from datetime import timedelta

TOP_NUM = 5
DEPART_LIMIT = 1200

class MRMostUsedWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer_init=self.init_reducer,
                   reducer=self.reducer_find_max_word,
                   reducer_final=self.reducer_final)
        ]

    def mapper_get_words(self, _, line):

        words = line.split(',')
        year = words[0].replace('"', '')
        if year == 'Year':  # skip header
            return
        orig = words[11].replace('"', '')
        dest = words[18].replace('"', '')
        carrier = words[8].replace('"', '')
        flight_date_str = words[5].replace('"', '')
        deptime = words[25].replace('"', '') # CRSDepTime
        #arrtime = words[35].replace('"', '') # CRSArrTime

        try:
            deptime = int(deptime)
            flight_date_datetime = datetime.datetime.strptime(flight_date_str, '%Y-%M-%d')

            carrier_delay = words[52].replace('"', '')
            weather_delay = words[53].replace('"', '')
            nas_delay = words[54].replace('"', '')
            security_delay = words[55].replace('"', '')
            late_aircraft_delay = words[56].replace('"', '')

            delay_list = [carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay]
            total_delay = sum([float(n) if n else 0 for n in delay_list])

            # as first leg
            # limit: before 12:00 PM
            if deptime < DEPART_LIMIT:
                yield (dest, str(flight_date_datetime)[:10]), (orig, 1, carrier, total_delay)

            # as second leg
            # limit:
            # 1. after 12:00 PM
            # 2. two days after first leg
            if deptime > DEPART_LIMIT:
                flight_date_datetime -= timedelta(days=2)
                yield (orig, str(flight_date_datetime)[:10]), (dest, 0, carrier, total_delay)

            #print 'org: {}, dest:{}, delay: {}'.format(orig, dest, total_delay)
            #yield (orig, dest, carrier), total_delay
        except Exception as e:
            print e
            pass

    def reducer_count_words(self, airport_and_date, orig_dest_carrier):

        transition, date = airport_and_date

        from_list = list()
        to_list = list()
        for (airport, direction, carrier, delay) in orig_dest_carrier:
            if direction:
                #print type(direction), direction
                from_list.append((airport, carrier, delay))
            else:
                to_list.append((airport, carrier, delay))

        if from_list and to_list:
            for org, org_carrier, first_delay in from_list:
                for dest, dest_carrier, second_delay in to_list:
                    yield (date, org, transition, dest), (first_delay+second_delay, org_carrier, dest_carrier)
        else:
            pass


    def init_reducer(self):
        self.to_insert = list()


    def reducer_find_max_word(self, date_and_route, delay_and_orig_dest_carrier):

        #print date_and_route, delay_and_orig_dest_carrier
        date, org, transition, des = date_and_route
        # we keep the top 5 performance options in database

        delay, org_carrier, dest_carrier = min(delay_and_orig_dest_carrier)

        #for (delay, org_carrier, dest_carrier) in reversed(sorted(delay_and_orig_dest_carrier)[:TOP_NUM]):
        self.to_insert.append([date, org, transition, des, org_carrier, dest_carrier, delay])
        yield (date, org, transition, des), (org_carrier, dest_carrier, delay)


    def reducer_final(self):

	import time
        start = time.time()

        import boto3
        import decimal


        from boto3.dynamodb.types import DYNAMODB_CONTEXT
        DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
        DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0

        from boto3.session import Session
        session = Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                          region_name='us-east-1')

        dynamodb = session.resource('dynamodb', region_name='us-east-1', endpoint_url="http://{}:8000".format(DYNAMODB_ENDPOINT_URL))

        table = dynamodb.Table('Group3_2')

        for dep_date, org, transition, des, org_carrier, dest_carrier, delay in self.to_insert:

            try:
                response = table.put_item(
                    Item={
                        'delay': decimal.Decimal(delay),
                        'origin_airport': org,
                        'transition_airport': transition,
                        'dest_airport': des,
                        'origin_carrier ': org_carrier,
                        'dest_carrier': dest_carrier,
                        'depart_date': int(dep_date[0:4]+dep_date[5:7]+dep_date[8:10])  #
                    }
                )
            except Exception as e:
                print e


        end = time.time()
        print 'Took {} secs inserting {} records'.format(end-start, len(self.to_insert))


if __name__ == '__main__':
    MRMostUsedWord.run()



