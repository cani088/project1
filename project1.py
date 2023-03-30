import json
import re
from concurrent import futures
import numpy as np
from scipy.stats import chisquare

# store stopwords in a hash map for faster search when filtering
stopWordsHash = {}

# categories_tokens is a hash map that stores a hash map of each unique token from the reviewText
#e.g
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

categories_tokens = {}
# tokens_stats is a hash map that stores for each token the amounts of times it has appeared in each category
# e.g
# {
#     'token1': {
#         'category1': 50,
#         'category2': 30
#     },
#     'token2': {
#         'category1': 22,
#         'category2': 23
#     }
# }
tokens_stats = {}

# totalDocuments keeps track of the number of documents that is later needed for calculating chi-squared
totalDocuments = 0

# store stopwords in a hash for faster search
with open('stopwords.txt', 'r') as file:
    for word in file.read().split("\n"):
        stopWordsHash[word] = 1


def tokenizeReview(review):
    review = json.loads(review)
    global totalDocuments
    totalDocuments += 1
    # this regex can be improved to reject single character words
    tokens = re.findall(r'\b[^\d\W]+\b|[()[]{}.!?,;:+=-_`~#@&*%€$§\/]^', review["reviewText"])
    tokens = [token.lower() for token in tokens if token.lower() not in stopWordsHash and len(token) > 1]
    for token in tokens:
        # if token not in tokens_stats:
        #     tokens_stats[token] = {
        #         'categories': {},
        #         'total_appeareances': 0
        #     }
        # if review['category'] in tokens_stats[token]['categories']:
        #     tokens_stats[token]['categories'][review['category']] += 1
        # else:
        #     tokens_stats[token]['categories'][review['category']] = 1

        if review['category'] not in categories_tokens:
            categories_tokens[review['category']] = {}

        categories_tokens[review['category']][token] = {}

    return {
        "category": review['category'],
        "tokens": tokens
    }

#Parallelization of the tokenization and filtering
with open('reviews_devset.json', 'r') as reviews:
    # with futures.ThreadPoolExecutor(max_workers=1000) as executor:
    #     fs = {executor.submit(tokenizeReview, review): review for review in reviews}
    for review in reviews:
        tokenizeReview(review)

# In order to be able to calculate chi-square, we need to have the following values for each token (c is refered to the category, t is referred to the token(word))
# A- number of documents in c which contain t
    # this is found in tokens_stats[{token}][{category}]
# B- number of documents not in c which contain t
    # we need to calculate the total appeareances for each of the categories in tokens_stats[{token}] and subtract A from it
# C- number of documents in c without t
    # this can be derived from getting the total number of documents for the category and subtracting A from it
# D- number of documents not in c without t
    # total number of documents minus total documents in c minus B
# N- total number of retrieved documents (can be omitted for ranking)
# the formula for calculating chi-squared is
# N(AD - BC)^2 / (A+B)(A+C)(B+D)(C+D)

print('done')