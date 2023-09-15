# the below is an example of a text classification model
# it involves classifying the subject category given the course title
# dataframe conversions:
#sdf = spark.createDataFrame(df)
#pdf = sdf.toPandas()
import pyspark.ml.feature
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import Tokenizer,StopWordsRemover,CountVectorizer,IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.types import StringType

#read the data

df = spark.read.format('csv').options(header='true', inferSchema='true').load('/FileStore/courses/courses_dataset.csv')
df.show()

#display the columns

df.columns

#select just the columns we need
#df.select('course_title','subject').show()

df = df.select('course_title','subject')

#check for any missing values in the coluns that interest us
df.toPandas()['subject'].isnull().sum()

#drop the missing values in the subject column
df = df.dropna(subset=('subject'))

#alternative handling
#df["subject"].fillna("NA", inplace = True)

### feature engineering starts here ###

# view the available features:
# tokenizer- convert sentence into word tokens ; CountVectorizer - converts given text into vectors (numbers); Extractor - extract various features from dataset
# IDF (inverse document freaquency - how important is a given work relative to the other words in this body of words)
dir(pyspark.ml.feature)

# add stages to the pipeline
tokenizer = Tokenizer(inputCol='course_title',outputCol='mytokens')
stopwords_remover = StopWordsRemover(inputCol='mytokens',outputCol='filtered_tokens')
vectorizer = CountVectorizer(inputCol='filtered_tokens',outputCol='rawFeatures')
idf = IDF(inputCol='rawFeatures',outputCol='vectorizedFeatures')

# vectorize the lable
labelEncoder = StringIndexer(inputCol='subject',outputCol='label').fit(df)

# add it to the df
df = labelEncoder.transform(df)

label_dict = {'Web Development':0.0,
 'Business Finance':1.0,
 'Musical Instruments':2.0,
 'Graphic Design':3.0}

#split the dataset
(trainDF,testDF) = df.randomSplit((0.7,0.3),seed=42)


#create the estimator
lr = LogisticRegression(featuresCol='vectorizedFeatures',labelCol='label')

#build pipeline
pipeline = Pipeline(stages=[tokenizer,stopwords_remover,vectorizer,idf,lr])

#build model
lr_model = pipeline.fit(trainDF)

predictions = lr_model.transform(testDF)

predictions.select('rawPrediction','probability','subject','label','prediction').show(10)

## model evaluation
evaluator = MulticlassClassificationEvaluator(labelCol='label',predictionCol='prediction',metricName='accuracy')
accuracy = evaluator.evaluate(predictions)

display(accuracy)

##single prediction##
ex1 = spark.createDataFrame([
    ("Building Machine Learning Apps with Python and PySpark",StringType())
],
["course_title"]

)

pred_ex1 = lr_model.transform(ex1)

pred_ex1.select('course_title','rawPrediction','probability','prediction').show()

