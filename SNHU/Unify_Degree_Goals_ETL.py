# Databricks notebook source
import pyodbc
import pandas as pd
import os

import json
import glob
import os
import time
from datetime import datetime
import re
import seaborn as sns

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn import metrics


# from IPython.core.interactiveshell import InteractiveShell
# InteractiveShell.ast_node_interactivity = "all"
# pd.options.display.max_columns = None
# pd.options.display.max_rows = None
# #pd.set_option('display.max_ colwidth', -1)
# pd.options.display.max_seq_items = 2000
# pd.set_option('display.float_format', '{:.0f}'.format)

# COMMAND ----------

####                             was used to create df_temp at top of script                              ####



#mport contractions
import warnings
import re
import string
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
punkt = string.punctuation
stop_word_list = ['why','background','misc','support','work', 'welcome','goals','yes','goal','motivation','completed','college','needs','discounts', ',', 'online,','start','program','motivation/goals','job/status','experience','support','concerns','aep','schools','educational','plan','tuition','schedule','education','snhu','funding','credits','intended','term','goals/motivation','intro','call','first','attempt']
#stop_word_list.remove('not')

#nlp =spacy.load("en_core_web_sm")


def clean_desc(text):
    #text = contractions.fix(text)
    #text = re.sub(r'https\S+', ' ', text).strip()
    #text = re.sub(r'www\S+', ' ', text)
    #text = re.sub(r"\d+"," ",text)
    #text = re.sub("\n|\r", " ",text)
    #text = re.sub(r"-+|\."," ",text)
    #text = re.sub(r"\([^()]*\)", " ", text)
    #text = "".join([str(char) for char in text if not char in punkt]).strip()
    text = " ".join([word for word in text.split() if word.lower() not in stop_word_list])
    return str(text).lower().strip()

# COMMAND ----------

#df_opt = pd.read_(r'C:\Users\j.lopez2\OneDrive - SNHU\Desktop\csv\unify_call_notes.xlsx')
#sdf = spark.read.csv('abfss://csv@snhulakehouse.dfs.core.windows.net/Advisor_2022-05-31T18_45_27.990Z.csv',header=True,inferSchema=True)
import pyspark.pandas as ps
#df_opt = ps.read_table("poc.bptest1.unify_call_notes")
df_opt = pd.read_excel(r'unify_call_notes.xlsx')

#sdf=spark.read.table("poc.bptest1.unify_call_notes")
#df_opt = sdf.toPandas()

# COMMAND ----------

#print(df_opt)

# COMMAND ----------

#logic that created df_temp

df_opt['split_text']=df_opt.Description.str.split('[\:\-\?\*\●]')
df_exp =df_opt.explode('split_text')

df_exp=df_exp[df_exp['split_text']!='']
df_exp=df_exp[df_exp['split_text']!=' ']
df_exp = df_exp[df_exp['split_text'].notna()]

df_exp['Clean_Description']=df_exp['split_text'].apply(clean_desc)
df_exp=df_exp[df_exp['Clean_Description']!='']
df_exp=df_exp[df_exp['Clean_Description']!=' ']
df_exp = df_exp[df_exp['Clean_Description'].notna()]

len(df_exp)
len(df_opt)
print(df_exp.to_string())

# COMMAND ----------

#df = df.reset_index()
#df['ts'] = pd.to_datetime(df['Timestamp'])
# 'ts' is now datetime of 'Timestamp', you just need to set it to index
#df = df.set_index('ts')
#
# df_opt = df_opt.reset_index()
#
# # COMMAND ----------
#
# arr =[]
# degrees=['ba ', 'bs ', 'bs.', 'ba.', 'as ', 'as.', 'ms ']
#
# for deg in degrees:
#     arr.append(df_exp[df_exp['Clean_Description'].str.startswith(str(deg))==True])
#
#
# # COMMAND ----------
#
# df_all_deg = ps.concat(arr)
# df_all_deg.Clean_Description.value_counts()
#
# # COMMAND ----------
#
# #spark.sql("DROP TABLE IF EXISTS poc.bptest1.unify_call_notes_cleaned")
#
# # COMMAND ----------
#
# df_all_deg.head(15)
# # Write the table to Unity Catalog.
# #df_all_deg.to_table("poc.bptest1.unify_call_notes_cleaned")
