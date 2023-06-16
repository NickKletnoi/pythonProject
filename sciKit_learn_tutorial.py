import json
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer
from sklearn import svm
file_name = './data/Books_small.json'
reviews = []

class Sentiment:
    NEGATIVE = 'NEGATIVE'
    NEUTRAL = 'NEUTRAL'
    POSITIVE = 'POSITIVE'

class Review:
    def __init__(self, text,score):
        self.text = text
        self.score = score
        self.sentiment = self.get_sentiment()

    def get_sentiment(self):
        if self.score <= 2:
            return Sentiment.NEGATIVE
        elif self.score ==3:
            return Sentiment.NEUTRAL
        else:
            return Sentiment.POSITIVE

with open(file_name) as f:
    for line in f:
        review = json.loads(line)
        reviews.append(Review(review["reviewText"],review["overall"]))

## split the set
training, test = train_test_split(reviews, test_size=0.33, random_state=42)
train_x = [x.text for x in training]
train_y = [x.sentiment for x in training]
test_x = [x.text for x in test]
test_y = [x.sentiment for x in test]

# vectorize the data
from sklearn.feature_extraction.text import CountVectorizer
vectorizer = CountVectorizer()
train_x_vectors = vectorizer.fit_transform(train_x)
test_x_vectors = vectorizer.transform(test_x)

## classify the data ## Linear SVM

clf_svm = svm.SVC(kernel='linear')
clf_svm.fit(train_x_vectors,train_y)

print(str(clf_svm.predict(test_x_vectors[0])))








