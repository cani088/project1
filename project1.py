#! /usr/bin/python

import json
from timeit import default_timer as timer
import re
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRWordFrequencyCount(MRJob):

    def map_words_categories(self, _, line):
        for review in line.splitlines():
            review = json.loads(review)
            tokens = re.findall(r'\b[^\d\W]+\b|[()[]{}.!?,;:+=-_`~#@&*%€$§\/]^', review["reviewText"])
            # tokens = re.findall(r'\b\w+\b|[(){}\[\].!?,;:+=\-_"\'`~#@&*%€$§\\/]+', review['reviewText'])
            tokens = list(set([token.lower() for token in tokens]))
            # self.writeToNewDevset(review['category'], tokens)
            for token in tokens:
                yield (review['category'], token), 1
          
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