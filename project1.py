import json
import re
import time

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRWordFrequencyCount(MRJob):
    # store stopwords in a hash map for faster search when filtering
    stopWordsHash: dict[str, int] = {}

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
    categories_chi: dict[str, list[dict[str, float]]] = {}
    testString = 'asdadadas'
    # totalDocuments keeps track of the number of documents that is later needed for calculating chi-squared
    totalDocuments: dict[str, int] = {}
    categories_counts = {}

    def tokenizeReview(self, review):
        # this regex can be improved to reject single character words
        tokens = re.findall(
            r'(?=\b\w{2,}\b)[\w()[\]{}.!?,;:+=\-_`~#@&*%€$§\/]+', review["reviewText"])
        # tokens = re.findall(r'\b\w+\b|[(){}\[\].!?,;:+=\-_"\'`~#@&*%€$§\\/]+', review['reviewText'])
        self.categories_tokens[review['category']] = {}
        tokens = list(set([token.lower() for token in tokens if token.lower() not in self.stopWordsHash]))
        # self.writeToNewDevset(review['category'], tokens)
        for token in tokens:
            yield review['category'], token
            # yield (review['category'], token), 1

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

    def mapper_count_words(self, _, line):
        for review in line.splitlines():
            review = json.loads(review)
            for out in self.tokenizeReview(review):
                yield out, 1
            yield ('category_count', review['category']), 1

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is, so we can easily use Python's max() function.
        yield word, sum(counts)

    def mapper_set_categories_tokens(self, line, count):
        category = line[0]
        word = line[1]
        if category not in self.categories_tokens:
            self.categories_tokens.__setitem__(category,{})
        self.categories_tokens[category].__setitem__(word, count)

    def reducer_calculate_chi(self):
        self.calculateChi()
        self.sortTokens()

        for category in self.categories_chi:
            value = category
            for token in self.categories_chi[category]:
                value += ' ' + token['token'] + ':' + str(token['chi'])
            yield value, ' '

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_count_words,
                reducer=self.reducer_count_words),
            MRStep(
                mapper=self.mapper_set_categories_tokens,   
                reducer_final=self.reducer_calculate_chi)
        ]
        
    def populate_stopwords(self):
        with open('stopwords.txt', 'r') as file:
            for word in file.read().split("\n"):
                self.stopWordsHash[word] = 1

if __name__ == '__main__':
    job = MRWordFrequencyCount();
    job.populate_stopwords();
    job.run();


