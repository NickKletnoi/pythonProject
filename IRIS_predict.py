from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from autosklearn.classification import AutoSklearnClassifier
import pickle


# dataset:
X, y = load_iris(return_X_y=True)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2)

# train model:
classifier = AutoSklearnClassifier(
    time_left_for_this_task=30,
    per_run_time_limit=60,
    memory_limit=1024*12) # depends on your computer
classifier.fit(X_train, y_train)

# save model
with open('iris-classifier.pkl', 'wb') as f:
    pickle.dump(classifier, f)

# load model
with open('iris-classifier.pkl', 'rb') as f:
    loaded_classifier = pickle.load(f)

# predict
y_true = y_test
y_pred = loaded_classifier.predict(X_test)
print('iris classifier: accuracy:', accuracy_score(y_true, y_pred))
# iris classifier: accuracy: 0.9333333333333333

#################################################################################################################
##prep

# !pip install numpy
# !pip install scipy
# !pip install scikit-learn
# !pip install auto-sklearn
# !pip install pickle5

# example of auto-sklearn for the sonar classification dataset
from pandas import read_csv
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
from autosklearn.classification import AutoSklearnClassifier
# load dataset
url = 'https://raw.githubusercontent.com/jbrownlee/Datasets/master/sonar.csv'
dataframe = read_csv(url, header=None)
# print(dataframe.head())
# split into input and output elements
data = dataframe.values
X, y = data[:, :-1], data[:, -1]
# minimally prepare dataset
X = X.astype('float32')
y = LabelEncoder().fit_transform(y.astype('str'))
# split into train and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=1)
# define search
model = AutoSklearnClassifier(time_left_for_this_task=5*60, per_run_time_limit=30, n_jobs=8)
# perform the search
model.fit(X_train, y_train)
# summarize
print(model.sprint_statistics())
# evaluate best model
y_hat = model.predict(X_test)
acc = accuracy_score(y_test, y_hat)
print("Accuracy: %.3f" % acc)

