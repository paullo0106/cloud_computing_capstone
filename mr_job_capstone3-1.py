
from mrjob.job import MRJob
from mrjob.step import MRStep
import re


TOP_NUM = 10000

class MRMostUsedWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]

    def mapper_get_words(self, _, line):

        words = line.split(',')

        #print words[6], words[14]  # orig, dest

        #yield words[6], 1
        orig = words[6].replace('"', '')
        dest = words[14].replace('"', '')

        if orig != 'Origin' and dest != 'Dest':  # skip destination
            yield orig, 1
	    yield dest, 1
            # yield dest, 1

        # yield each word in the line
        #for word in WORD_RE.findall(line):
        #    yield (word.lower(), 1)

    def combiner_count_words(self, word, counts):
        # optimization: sum the words we've seen so far
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), word)

    # discard the key; it is just None
    def reducer_find_max_word(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word

	rank = 0
        for (number, airport) in sorted(word_count_pairs, reverse=True)[:TOP_NUM]:
	    rank+=1
	    yield rank, number


if __name__ == '__main__':
    MRMostUsedWord.run()



