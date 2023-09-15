from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import OrdinalEncoder
import pandas as pd
import pickle
import os

# Load dataset to test out the prediction
df_bank = pd.read_csv('bank1.csv')

# load the model from disk
os.chdir(r'C:\Users\kletn\PycharmProjects\PythonProject\LinearReg')
#loaded_model = pickle.load(open('model1.pkl', 'rb'))

# Drop 'duration' column
df_bank = df_bank.drop('duration', axis=1)

# Copying original dataframe
df_bank_ready = df_bank.copy()

scaler = StandardScaler()
num_cols = ['age', 'balance', 'day', 'campaign', 'pdays', 'previous']
df_bank_ready[num_cols] = scaler.fit_transform(df_bank_ready[num_cols])

oen = OrdinalEncoder()
cat_cols = ['job', 'marital', 'education', 'default', 'housing', 'loan', 'contact', 'month', 'poutcome']
df_bank_ready[cat_cols] = oen.fit_transform(df_bank_ready[cat_cols])

# Select Features
feature = df_bank_ready

# Loading model from a joblib file
#loaded_model = load('bank_deposit_classification.joblib')

# Loading model from a pickle
loaded_model = pickle.load(open("bank_deposit_classification.pkl", "rb"))

df_bank['deposit_prediction'] = loaded_model.predict(feature)
df_bank['deposit_prediction'] = df_bank['deposit_prediction'].apply(lambda x: 'yes' if x==0 else 'no')

print(df_bank.to_string())




