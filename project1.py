#! /usr/bin/python

import json
from timeit import default_timer as timer
import re
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRWordFrequencyCount(MRJob):
    categories_tokens = {'category_count': {}}

    categories_counts = {
        "Apps_for_Android":	2638,
        "Automotive":	1374,
        "Baby":	916,
        "Beauty":	2023,
        "Book":	22507,
        "CDs_and_Vinyl":	3749,
        "Cell_Phones_and_Accessorie":	3447,
        "Clothing_Shoes_and_Jewelry":	5749,
        "Digital_Music":	836,
        "Electronic":	7825,
        "Grocery_and_Gourmet_Food":	1297,
        "Health_and_Personal_Care":	2982,
        "Home_and_Kitche":	4254,
        "Kindle_Store":	3205,
        "Movies_and_TV":	4607,
        "Musical_Instrument":	500,
        "Office_Product":	1243,
        "Patio_Lawn_and_Garde":	994,
        "Pet_Supplie":	1235,
        "Sports_and_Outdoor":	3269,
        "Tools_and_Home_Improvement":	1926,
        "Toys_and_Game":	2253,
    }

    def map_get_categories(self, _, line):
        for review in line.splitlines():
            review = json.loads(review)
            yield review['category'], 1

    # def reducer_count_categories(self):


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
          
    def reducer_count_words(self, word, counts):
        yield word, sum(counts)

    def mapper_set_categories_tokens(self, split, count):
        # from the first step's output, we construct a dictinary that we are going to use for calculating chi values
        self.categories_tokens[split[0]].__setitem__(split[1], count)

    def steps(self):
        return [
            # MRStep(mapper=self.map_get_categories,
            #        reducer=self.reducer_count_words,
            #     #    reducer_final=self.reducer_count_categories
            #        ),
            MRStep(mapper=self.map_words_categories,
                   reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_set_categories_tokens)
        ]

if __name__ == '__main__':
    job = MRWordFrequencyCount()
    job.run()