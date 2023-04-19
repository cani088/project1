import json
import re
import string
from timeit import default_timer as timer

from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import os


class MRWordFrequencyCount(MRJob):
    # store stopwords in a hash map for faster search when filtering
    stopWordsHash: dict[string, int] = {}
    # self.categories_tok
    # ens is a hash map that stores a hash map of each unique token from the reviewText
    # e.g
    # {
    #     'category1': {
    #         'token1': 12,
    #         'token2': 23
    #     },
    #     'category2': {
    #         'token1': 22,
    #         'token2': 3
    #     }
    # }
    # categories_tokens: dict[string, dict[string, int]] = {}
    categories_tokens = {'category_count': {}}
    categories_chi: dict[string, list[dict[string, float]]] = {}
    outputPath = os.path.abspath('output.txt').replace('\\', '/')
    logPath = os.path.abspath('log.txt').replace('\\', '/')

    # totalDocuments keeps track of the number of documents that is later needed for calculating chi-squared
    totalDocuments: dict[string, int] = {}
    categories_counts = {}

    def tokenizeReview(self, review):
        # this regex can be improved to reject single character words
        tokens = re.findall(
            r'\b[^\d\W]+\b|[()[]{}.!?,;:+=-_`~#@&*%€$§\/]^', review["reviewText"])
        # tokens = re.findall(r'\b\w+\b|[(){}\[\].!?,;:+=\-_"\'`~#@&*%€$§\\/]+', review['reviewText'])
        self.categories_tokens[review['category']] = {}
        tokens = list(set([token.lower() for token in tokens if token.lower() not in self.stopWordsHash]))
        # self.writeToNewDevset(review['category'], tokens)
        for token in tokens:
            yield review['category'], token
            # yield (review['category'], token), 1

    def initFiles(self):
        with open('stopwords.txt', 'r') as file:
            for word in file.read().split("\n"):
                self.stopWordsHash[word] = 1
        self.logData([json.dumps(self.stopWordsHash)])

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

    def map_words_categories(self, _, line):
        for review in line.splitlines():
            review = json.loads(review)

            for category_token_tuple in self.tokenizeReview(review):
                # category_token_tuple in this case is a tuple, consisting of (category_name, token)
                yield category_token_tuple, 1

            # yield category count for each of the articles to have total amount of articles for each category
            yield ('category_count', review['category']), 1

    def reducer_count_words(self, word, counts):
        yield word, sum(counts)

    def mapper_set_categories_tokens(self, split, count):
        # from the first step's output, we construct a dictinary that we are going to use for calculating chi values
        self.categories_tokens[split[0]].__setitem__(split[1], count)


    def reducer_calculate_chi(self):
        self.calculateChi()
        self.sortTokens()

        # with open(self.outputPath, 'w') as file:
        for category in self.categories_chi:
            append = ''
            for token in self.categories_chi[category]:
                append += ' ' + token['token'] + ':' + str(token['chi'])
            # append += "\n"
            # file.write(append)
            yield category, append

    def logData(self, data):
        with open(self.logPath, 'a') as file:
            for item in data:
                file.write(str(item) + "\n")

    def steps(self):
        return [
            MRStep(mapper=self.map_words_categories,
                   reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_set_categories_tokens,
                   reducer_final=self.reducer_calculate_chi)
        ]

if __name__ == '__main__':
    start = timer()
    job = MRWordFrequencyCount()
    job.initFiles()
    job.run()
    end = timer()
    print(end - start)