#! /usr/bin/python

import json
from timeit import default_timer as timer
import re
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRWordFrequencyCount(MRJob):
    categories_tokens = {'category_count': {}}

    def map_words_categories(self, _, line):
        for review in line.splitlines():
            review = json.loads(review)
            tokens = re.findall(r'\b[^\d\W]+\b|[()[]{}.!?,;:+=-_`~#@&*%€$§\/]^', review["reviewText"])
            # tokens = re.findall(r'\b\w+\b|[(){}\[\].!?,;:+=\-_"\'`~#@&*%€$§\\/]+', review['reviewText'])
            self.categories_tokens[review['category']] = {}
            tokens = list(set([token.lower() for token in tokens]))
            # self.writeToNewDevset(review['category'], tokens)
            for token in tokens:
                yield (review['category'], token), 1
            yield ('category_count', review['category']), 1
          
    def reducer_count_words(self, word, counts):
        yield word, sum(counts)

    def mapper_set_categories_tokens(self, split, count):
        # from the first step's output, we construct a dictinary that we are going to use for calculating chi values
        self.categories_tokens[split[0]].__setitem__(split[1], count)

    def steps(self):
        return [
            MRStep(mapper=self.map_words_categories,
                   reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_set_categories_tokens)
        ]

if __name__ == '__main__':
    job = MRWordFrequencyCount()
    job.run()