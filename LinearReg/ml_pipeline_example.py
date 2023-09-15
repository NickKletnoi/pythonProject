import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from sklearn.model_selection import GridSearchCV, RepeatedStratifiedKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler, MinMaxScaler
from sklearn.ensemble import RandomForestClassifier

df_bank = pd.read_csv('bank.csv')
X = df_bank.drop('deposit', axis=1)
y = df_bank['deposit']

transformers_list = [
('encode', MinMaxScaler(dtype='int',drop='duration'),['job', 'marital', 'education', 'default', 'housing', 'loan', 'contact', 'month', 'poutcome']),
('scale', StandardScaler(), ['age', 'balance', 'day', 'campaign', 'pdays', 'previous']),
(['deposit'])
]

column_transformer = ColumnTransformer(transformers_list)
transformed_raw = column_transformer.fit_transform(df_bank)

features = pd.DataFrame(
    transformed_raw,
    columns=column_transformer.get_feature_names_out()
)

features.head()



