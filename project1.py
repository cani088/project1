#! /usr/bin/python

import json
from timeit import default_timer as timer
import re
from mrjob.job import MRJob
from mrjob.step import MRStep
import os

class MyJob(MRJob):
    tokens = []
    logPath = os.path.abspath('log.txt').replace('\\', '/')
    stopWordsPath = os.path.abspath('stopwords.txt').replace('\\', '/')
    outputPath = os.path.abspath('output.txt').replace('\\', '/')
    categories_chi = {}

    stopWordsHash = {}
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

    categories_tokens = {
        "Apps_for_Android":	{},
        "Automotive":	{},
        "Baby":	{},
        "Beauty":	{},
        "Book":	{},
        "CDs_and_Vinyl":	{},
        "Cell_Phones_and_Accessorie":	{},
        "Clothing_Shoes_and_Jewelry":	{},
        "Digital_Music":	{},
        "Electronic":	{},
        "Grocery_and_Gourmet_Food":	{},
        "Health_and_Personal_Care":	{},
        "Home_and_Kitche":	{},
        "Kindle_Store":	{},
        "Movies_and_TV":	{},
        "Musical_Instrument":	{},
        "Office_Product":	{},
        "Patio_Lawn_and_Garde":	{},
        "Patio_Lawn_and_Garde2":	{},
        "Pet_Supplie":	{},
        "Sports_and_Outdoor":	{},
        "Tools_and_Home_Improvement":	{},
        "Toys_and_Game":	{},
        'category_count': {}
    }

    def initFiles(self):
        with open(self.stopWordsPath, 'r') as file:
            for word in file.read().split("\n"):
                self.stopWordsHash[word] = 1

    def map_words_categories(self, _, line):
        for review in line.splitlines():
            review = json.loads(review)
            tokens = re.findall(r'\b[^\d\W]+\b|[()[]{}.!?,;:+=-_`~#@&*%€$§\/]^', review["reviewText"])
            # tokens = re.findall(r'\b\w+\b|[(){}\[\].!?,;:+=\-_"\'`~#@&*%€$§\\/]+', review['reviewText'])
            tokens = set(token.lower() for token in tokens if token not in self.stopWordsHash and len(token) > 2)
            for token in tokens:
                yield (review['category'], token), 1
            
            yield ('category_count', review['category']), 1

    def combiner_count_words(self, word, counts):
        yield word, sum(counts)

    def reducer_count_words(self, word, counts):
        totalCount = sum(counts)

        yield word, totalCount


    def logData(self, data):
        with open(self.logPath, 'a') as file:
            for item in data:
                file.write(str(item) + "\n")


    def calculateChi(self):
        # c is refered to the category, t is referred to the token(word)
        # In order to be able to calculate chi-square, we need to have the following values for each token:
        # N- total number of retrieved documents
        # A- number of documents in c which contain t - this value is found in self.categories_tokens[{c}][{t}]
        # B- number of documents not in c which contain t - we need go onto each category and check if they have the token, if yes, we accumulate its value into token's B
        # C- number of documents in c without t - this can be derived from getting the total number of documents for the category and subtracting A from it
        # D- number of documents not in c without t - N minus total documents in c minus calculated B of each category
        # the formula for calculating chi-squared is
        # N(AD - BC)^2 / (A+B)(A+C)(B+D)(C+D)
        N = 0

        for c in self.categories_tokens['category_count']:
            N += self.categories_tokens['category_count'][c]
            self.categories_counts[c] = self.categories_tokens['category_count'][c]

        # remove category_count
        self.categories_tokens.pop('category_count')

        for category in self.categories_tokens:
            self.categories_chi[category] = []
            for token in self.categories_tokens[category]:
                A = self.categories_tokens[category][token]
                B = 0

                for c in self.categories_tokens:
                    if c == category:
                        continue
                    if token in self.categories_tokens[c]:
                        B += self.categories_tokens[c][token]

                C: int = self.categories_counts[category] - A
                D: int = N - self.categories_counts[category] - B

                # R = N(AD - BC)^2 / (A+B)(A+C)(B+D)(C+D)
                R: float = (N * (((A * D) - (B * C)) ** 2)) / ((A + B) * (A + C) * (B + D) * (C + D))
                self.categories_chi[category].append({"token": token, "chi": R})


    def sortTokens(self):
        for category in self.categories_chi:
            self.categories_chi[category].sort(key=lambda x: x['chi'], reverse=True)
            if len(self.categories_chi[category]) > 76:
                self.categories_chi[category] = self.categories_chi[category][0:75]


    def calculate(self):
        self.calculateChi()
        self.sortTokens()

        for category in self.categories_chi:
            append = ''
            for token in self.categories_chi[category]:
                append += ' ' + token['token'] + ':' + str(token['chi'])
            yield category, append


    def second_mapper(self, word, count):
        yield None, (word[0], word[1], count)


    def second_reducer(self, word, word_counts):
        for cat, tok, count in word_counts:
            self.categories_tokens[cat][tok] = count

        for category, token in self.calculate():
            yield category, token

    def steps(self):
        return [
            MRStep(
                # mapper_init=self.initFiles,
                mapper=self.map_words_categories,
                combiner=self.combiner_count_words,
                reducer=self.reducer_count_words,
            ),
            MRStep(
                mapper=self.second_mapper,
                reducer=self.second_reducer
            )
        ]


if __name__ == '__main__':
    job = MyJob()
    job.run()
    # job.calculate(job.categories_tokens)

    # with job.make_runner() as runner:
    #     runner.run()
    #     global_output = job.categories_tokens
    #     job.calculate(global_output)