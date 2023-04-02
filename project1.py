import json
import re
import string
import time
from typing import Dict, Any

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRWordFrequencyCount(MRJob):
    # store stopwords in a hash map for faster search when filtering
    stopWordsHash: dict[string, int] = {}

    # self.categories_tokens is a hash map that stores a hash map of each unique token from the reviewText
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
    categories_tokens = {}
    categories_chi: dict[string, list[dict[string, float]]] = {}
    testString = 'asdadadas'
    # totalDocuments keeps track of the number of documents that is later needed for calculating chi-squared
    totalDocuments: dict[string, int] = {}

    def __int__(self):
        # store stopwords in a hash for faster search
        with open('stopwords.txt', 'r') as file:
            for word in file.read().split("\n"):
                self.stopWordsHash[word] = 1

    def tokenizeReview(self, review):
        review = json.loads(review)

        if review['category'] not in self.totalDocuments:
            self.totalDocuments[review['category']] = 0

        self.totalDocuments[review['category']] += 1
        self.categories_tokens['Patio_Lawn_and_Garde'] = review['category']

        # this regex can be improved to reject single character words
        tokens = re.findall(r'\b[^\d\W]+\b|[()[]{}.!?,;:+=-_`~#@&*%€$§\/]^', review["reviewText"])
        tokens = list(
            set([token.lower() for token in tokens if token.lower() not in self.stopWordsHash and len(token) > 1]))
        for token in tokens:
            yield review['category'] + '-' + token, 1

    def calculateChi(self):
        # In order to be able to calculate chi-square, we need to have the following values for each token (c is refered to the category, t is referred to the token(word))
        # A- number of documents in c which contain t
        # this is found in self.categories_tokens[{category}][{token}]
        # B- number of documents not in c which contain t
        # we need to calculate the total appeareances for each of the categories in tokens_stats[{token}] and subtract A from it
        # C- number of documents in c without t
        # this can be derived from getting the total number of documents for the category and subtracting A from it
        # D- number of documents not in c without t
        # total number of documents minus total documents in c minus B
        # N- total number of retrieved documents (can be omitted for ranking)
        # the formula for calculating chi-squared is
        # N(AD - BC)^2 / (A+B)(A+C)(B+D)(C+D)

        N = 0
        for cat in self.totalDocuments:
            N += self.totalDocuments[cat]

        for category in self.categories_tokens:
            for token in self.categories_tokens[category]:
                A = self.categories_tokens[category][token]
                B = 0
                D = 0
                for c in self.categories_tokens:
                    if c == category:
                        continue
                    if token in self.categories_tokens[c]:
                        add = self.categories_tokens[c][token]
                        B += add
                        # Add the remainder to D
                        D += self.totalDocuments[c] - add
                    else:
                        # Add all documents of C since they do not contain the token
                        D += self.totalDocuments[c]

                C: int = self.totalDocuments[category] - A
                # R = N(AD - BC)^2 / (A+B)(A+C)(B+D)(C+D)
                R: float = (N * (((A * D) - (B * C)) ^ 2)) / (A + B) * (A + C) * (B + D) * (C + D)
                self.categories_chi[category].append({"token": token, "chi": R})

    def sortTokens(self):
        for category in self.categories_chi:
            self.categories_chi[category].sort(key=lambda x: x.chi, reverse=True)
            self.categories_chi[category] = self.categories_chi[0, 74]

    def mapper_count_words(self, _, line):
        for review in line.splitlines():
            for out in self.tokenizeReview(review):
                yield out

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is, so we can easily use Python's max() function.
        yield word, sum(counts)


    def mapper_set_categories_tokens(self, _, line):
        for count in line:
            split = _.split('-')
            if split[0] not in self.categories_tokens:
                self.categories_tokens[str(split[0])][split[1]] = count

            self.categories_tokens[str(split[0])][str(split[1])] = count


    def reducer_calculate_chi(self, _, pairs):
        self.calculateChi()
        self.sortTokens()

        for category in self.categories_chi:
            str = ''
            for token in self.categories_tokens[category]:
                str += ' ' + token['token'] + ':' + str(token['chi'])
            yield category, str

    def steps(self):
        return [
            MRStep(mapper=self.mapper_count_words,
                   reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_set_categories_tokens,
                   reducer=self.reducer_calculate_chi)
        ]


if __name__ == '__main__':
    MRWordFrequencyCount.run()
