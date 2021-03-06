
from mrjob.job import MRJob
from mrjob.step import MRStep
from my_credential import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DYNAMODB_ENDPOINT_URL

TOP_NUM = 10

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

        carrier_delay = words[52].replace('"', '')
        weather_delay = words[53].replace('"', '')
        nas_delay = words[54].replace('"', '')
        security_delay = words[55].replace('"', '')
        late_aircraft_delay = words[56].replace('"', '')

        #print carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay
        delay_list = [carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay]
        if not any(delay_list):
            return

        # sum up delay type
        try:
            total_delay = sum([float(n) for n in delay_list])
            #print 'org: {}, dest:{}, delay: {}'.format(orig, dest, total_delay)
            yield (orig, dest, carrier), total_delay
        except Exception as e:
            print e
            pass

    def reducer_count_words(self, orig_and_dest_and_carrier, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.

        orig, dest, carrier = orig_and_dest_and_carrier

        cnt = 0
        total_delay = 0
        for delay in counts:
            total_delay += delay
            cnt += 1

        avg_delay = total_delay / float(cnt)  # reduce(lambda x, y: x + y, delay) / float(sum(cnt))
        #print orig, avg_delay, dest

        yield (orig, dest), (avg_delay, carrier)


    def init_reducer(self):
        self.to_insert = dict()


    def reducer_find_max_word(self, orig_and_dest, delay_and_carrier):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word

        orig, dest = orig_and_dest
        for (delay, carrier) in reversed(sorted(delay_and_carrier)[:TOP_NUM]):
            self.to_insert[(orig, dest, carrier)] = (delay)
            yield (orig, dest), (carrier, delay)


    def reducer_final(self):
	
        import boto3
        import decimal

        # http://github.com/boto/boto/issues/1531
        from boto3.dynamodb.types import DYNAMODB_CONTEXT
        DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
        DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0

        from boto3.session import Session

	import time

	start = time.time()

        session = Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                          region_name='us-east-1')

        dynamodb = session.resource('dynamodb', region_name='us-east-1', endpoint_url="http://{}:8000".format(DYNAMODB_ENDPOINT_URL))

        table = dynamodb.Table('Delay2_3')

        for k in self.to_insert:
            orig, dest, carrier = k
            delay = self.to_insert[k]

            #print orig, dest, self.to_insert[k]
            try:
                response = table.put_item(
                    Item={
                        'delay': decimal.Decimal(delay),
                        'origin_airport': orig,
                        'dest_airport': dest,
                        'carrier': carrier
                    }
                )
            except Exception as e:
                print e

        end = time.time()
        print 'Took {} secs inserting {} records'.format(end-start, len(self.to_insert.keys()))

	

if __name__ == '__main__':
    MRMostUsedWord.run()



