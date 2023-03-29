import json
import re
from concurrent import futures

data = {}
stopWordsHash = {}
with open('stopwords.txt', 'r') as file:
    for word in file.read().split("\n"):
        stopWordsHash[word] = 1


def tokenizeReview(review):
    review = json.loads(review)
    # the regex can be improved to reject single character words
    tokens = re.findall(r'\b[^\d\W]+\b|[()[]{}.!?,;:+=-_`~#@&*%€$§\/]^', review["reviewText"])
    return {
        "category": review['category'],
        "tokens": [token.lower() for token in tokens if token.lower() not in stopWordsHash and len(token) > 1]
    }

#Parallelization of the tokenization and filtering
with open('reviews_devset.json', 'r') as reviews:
    with futures.ThreadPoolExecutor(max_workers=1000) as executor:
        fs = {executor.submit(tokenizeReview, review): review for review in reviews}
        for f in futures.as_completed(fs):
            res = f.result()
            # group tokens by category
            if res['category'] not in data:
                data[res['category']] = [res['tokens']]
            else:
                data[res['category']].append(res['tokens'])

print(data)