import json
import re
from concurrent import futures

data = []
stopWordsHash = {}
with open('stopwords.txt', 'r') as file:
    for word in file.read().split("\n"):
        stopWordsHash[word] = 1


def tokenizeReview(review):
    review = json.loads(review)
    tokens = re.findall(r'\b[^\d\W]+\b|[()[]{}.!?,;:+=-_`~#@&*%€$§\/]^', review["reviewText"])
    return [token.lower() for token in tokens if token.lower() not in stopWordsHash and len(token) > 1]


with open('reviews_devset.json', 'r') as reviews:
    with futures.ThreadPoolExecutor(max_workers=1000) as executor:
        fs = {executor.submit(tokenizeReview, review): review for review in reviews}
        for f in futures.as_completed(fs):
            data.append(f.result())

print(data)