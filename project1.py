import json
import re
import time

# store stopwords in a hash map for faster search when filtering
stopWordsHash = {}

# categories_tokens is a hash map that stores a hash map of each unique token from the reviewText
# e.g
# {
#     'category1': {
#         'token1': {
#           'A': 0,
#           'B': 0,
#           'C': 0,
#           'D': 0,
#         },
#         'token2': {
#           'A': 0,
#           'B': 0,
#           'C': 0,
#           'D': 0,
#         }
#     },
#     'category2': {
#           ...
#     }
# }

categories_tokens = {}

# totalDocuments keeps track of the number of documents that is later needed for calculating chi-squared
totalDocuments = {}

# store stopwords in a hash for faster search
with open('stopwords.txt', 'r') as file:
    for word in file.read().split("\n"):
        stopWordsHash[word] = 1


def tokenizeReview(review):
    review = json.loads(review)
    global totalDocuments

    if review['category'] not in categories_tokens:
        categories_tokens[review['category']] = {}

    if review['category'] not in totalDocuments:
        totalDocuments[review['category']] = 0

    totalDocuments[review['category']] += 1
    # this regex can be improved to reject single character words
    tokens = re.findall(r'\b[^\d\W]+\b|[()[]{}.!?,;:+=-_`~#@&*%€$§\/]^', review["reviewText"])
    tokens = list(set([token.lower() for token in tokens if token.lower() not in stopWordsHash and len(token) > 1]))
    for token in tokens:
        if token not in categories_tokens[review['category']]:
            categories_tokens[review['category']][token] = {
                'A': 0,
                'B': 0,
                'C': 0,
                'D': 0,
                'R': 0
            }

        categories_tokens[review['category']][token]['A'] += 1


def calculateChi():
    # the formula for calculating chi-squared is
    # R = N(AD - BC)^2 / (A+B)(A+C)(B+D)(C+D)
    # In order to be able to calculate chi-square, we need to have the following values for each token (c is refered to the category, t is referred to the token(word))
    # A- number of documents in c which contain t
        # this is found in categories_tokens[{category}][{token}]
    # B- number of documents not in c which contain t
        # we need to calculate the total appeareances for each of the categories in tokens_stats[{token}] and subtract A from it
    # C- number of documents in c without t
        # this can be derived from getting the total number of documents for the category and subtracting A from it
    # D- number of documents not in c without t
        # total number of documents minus total documents in c minus B
    # N- total number of retrieved documents (can be omitted for ranking)


    N = 0
    for cat in totalDocuments:
        N += totalDocuments[cat]

    for category in categories_tokens:
        for token in categories_tokens[category]:
            B = 0
            D = 0
            for c in categories_tokens:
                if c == category:
                    continue
                if token in categories_tokens[c]:
                    add = categories_tokens[c][token]['A']
                    B += add
                    # Add the remainder to D
                    D += totalDocuments[c] - add
                else:
                    # Add all documents of C since they do not contain the token
                    D += totalDocuments[c]

            categories_tokens[category][token]['B'] = B
            categories_tokens[category][token]['D'] = D
            categories_tokens[category][token]['C'] = totalDocuments[category] - categories_tokens[category][token]['A']
            T = categories_tokens[category][token]
            # R = N(AD - BC)^2 / (A+B)(A+C)(B+D)(C+D)
            categories_tokens[category][token]['R'] = \
                (N * (((T['A'] * T['D']) - (T['B'] * T['C'])) ^ 2)) / (T['A'] + T['B']) * (T['A'] + T['C']) * (
                            T['B'] + T['D']) * (T['C'] + T['D'])
    print('finished')


# # Parallelization of the tokenization and filtering
start = time.time()

with open('reviews_devset.json', 'r') as reviews:
    # with futures.ThreadPoolExecutor(max_workers=1000) as executor:
    #     fs = {executor.submit(tokenizeReview, review): review for review in reviews}
    for review in reviews:
        tokenizeReview(review)
    calculateChi()


end = time.time()
print(end - start)