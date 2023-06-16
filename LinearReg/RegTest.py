import pandas as pd
import pickle
import os

# Changing the current working directory
os.chdir(r'C:\Users\kletn\PycharmProjects\PythonProject\data')
# Importing the dataset
dataset = pd.read_csv('Salary_Data.csv')
X = dataset.iloc[:, :-1].values
y = dataset.iloc[:, 1].values

from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 0)


# # vectorize the data
# from sklearn.feature_extraction.text import CountVectorizer
# vectorizer = CountVectorizer()
# train_x_vectors = vectorizer.fit_transform(train_x)
# test_x_vectors = vectorizer.transform(test_x)


# Fitting Simple Linear Regression to the Training Set
from sklearn.linear_model import LinearRegression
model = LinearRegression()
model.fit(X_train, y_train)

os.chdir(r'C:\Users\kletn\PycharmProjects\PythonProject\LinearReg')
with open('model1.pkl', 'wb') as f:
    pickle.dump(model, f)

y_hats = model.predict(X)

compare_df = pd.DataFrame(X)
compare_df['original_value'] = y
compare_df['predicted_result'] = y_hats

compare_df.rename(
    columns={0: "Years"},
    inplace=True,
)

print(compare_df)
#print(X_test)



