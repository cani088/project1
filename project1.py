#! /usr/bin/python

import json

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRWordFrequencyCount(MRJob):

    def map_words_categories(self, _, line):
        for review in line.splitlines():
            review = json.loads(review)
            yield review['category'], 1
          
    def reducer_count_words(self, word, counts):
        yield word, sum(counts)


    def steps(self):
        return [
            MRStep(mapper=self.map_words_categories,
                   reducer=self.reducer_count_words)
        ]

if __name__ == '__main__':
    job = MRWordFrequencyCount()
    job.run()