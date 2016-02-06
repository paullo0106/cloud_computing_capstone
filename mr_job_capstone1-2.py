
from mrjob.job import MRJob
from mrjob.step import MRStep
#from mrjob.protocol import PickleProtocol

TOP_NUM = 10

class MRMostUsedWord(MRJob):

    #INTERNAL_PROTOCOL = PickleProtocol

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]

    def mapper_get_words(self, _, line):

        words = line.split(',')

        year = words[0].replace('"', '')
        if year == 'Year':  # skip header
            return

        orig = words[11].replace('"', '')
	carrier = words[8].replace('"', '')

	if len(words) < 57:
		return

	try:
        	carrier_delay = words[52].replace('"', '')
        	weather_delay = words[53].replace('"', '')
        	nas_delay = words[54].replace('"', '')
        	security_delay = words[55].replace('"', '')
       		late_aircraft_delay = words[56].replace('"', '')
	except Exception:
		return

        #print carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay
        delay_list = [carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay]
        if not any(delay_list):
            return

        # sum up delay type
        try:
            total_delay = sum([float(n) for n in delay_list])

            #print 'org: {}, delay: {}'.format(orig, total_delay)
            yield carrier, total_delay
        except Exception as e:
            #print e
            pass

    '''
    def combiner_count_words(self, airport, counts):
        # optimization: sum the words we've seen so far
        print 'debugging1'
        yield (airport, counts)
        # yield (airport, sum(counts))
    '''

    def reducer_count_words(self, airport, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        #print 'debugging2'

        cnt = 0
        total_delay = 0
        for delay in counts:
            total_delay += delay
            cnt += 1

        avg_delay = total_delay / float(cnt)  # reduce(lambda x, y: x + y, delay) / float(sum(cnt))
        #print airport, avg_delay
        yield None, (avg_delay, airport)

    # discard the key; it is just None
    def reducer_find_max_word(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word

        for (number, airport) in sorted(word_count_pairs)[:TOP_NUM]:
            yield number, airport


if __name__ == '__main__':
    MRMostUsedWord.run()



