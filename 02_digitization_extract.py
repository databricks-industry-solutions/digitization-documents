# Databricks notebook source
# MAGIC %md
# MAGIC # Tika text extraction
# MAGIC Using [TikaInputFormat](https://github.com/databrickslabs/tika-ocr) library and tesseract binaries installed on each executor as an init script (optional), we can read any unstructured text as-is, extracting content type, text and metadata. Although this demo only focuses on PDF documents, Tika [supports](https://tika.apache.org/1.10/formats.html) literally any single MIME type, from email, pictures, xls, html, powerpoints, scanned images, etc. 

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC Given our utility library installed on your cluster as an external maven dependency, we abstracted most of its complexity away through a simple operation, `spark.read.format('tika')`. This command will issue similar operation as `binaryFile` but will also extract all text available and document metadata (such as author, name, permissions, etc.)

# COMMAND ----------

tika_df = spark.read.format('tika').load(landing_zone_fs)
display(tika_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploring page structure
# MAGIC Our first approach will be to explore the different structures across all different pages. While we expect some pages to be quite verbose (a well known challenge in ESG analysis), we expect others to be in tabular format. In this section, we will demonstrate a simple albeit powerful technique some practitioners have used for many years since the early days of exploratory data analysis. When a column of data is summarized into mask frequency counts, a process commonly known as *data profiling*, it can offer rapid insights into common structures and content, hence reveal how the raw data was encoded. Consider the following mask where alpha characters are replaced by `A`, numerical characters by `9` and symbols by `W`.

# COMMAND ----------

from pyspark.sql.functions import udf
import re
import unidecode


@udf('string')
def gen_mask(text):
  text = text.lower()
  text = unidecode.unidecode(text) # transliterates any unicode string into the closest possible representation in ascii text
  text = re.sub('[\s\t\n]+', 'S', text)
  text = re.sub('[a-z]+', 'A', text)
  text = re.sub('[0-9]+', '9', text)
  text = re.sub('[^AS9]+', 'W', text)
  return text

# COMMAND ----------

# MAGIC %md
# MAGIC It seems like a very simple transformation at first glance but offers powerful insights about hidden structures. For instance, understanding that a column may contain social security numbers becomes as simple as "eyeballing" a sequence 99-999-9999. Although we work here with a much larger text than different variations of phone numbers, IP addresses or social security numbers, we may still expect tabular formats to "stand out" given their characteristic patterns. 

# COMMAND ----------

ngram_df = tika_df.withColumn('mask', gen_mask('contentText')).toPandas()
display(ngram_df[['path', 'contentText', 'mask']])

# COMMAND ----------

# MAGIC %md
# MAGIC We invite our readers to scroll that list and "train their eyes" to spot main structural differences. For more details (and for a shameless plug), please refer to the excellent (albeit dated) book [Mastering Spark for Data Science](https://www.amazon.com/Mastering-Spark-Science-Andrew-Morgan/dp/1785882147) written by practitioners for practitioners - 2 authors being now databricks employees.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Separating document types
# MAGIC Although we are able to "eyeball" some changes between documents, unsupervised learning models such as KMeans should be able to capture more subtle variations of the different text structures, programmatically. The goal will not be to build the most accurate model, but rather delegate that business logic to find similar content and tag these appropriately as table vs. text. We start by generating ngrams of different sizes to capture descriptive structures in our corpus.

# COMMAND ----------

from sklearn.feature_extraction.text import TfidfVectorizer
vectorizer = TfidfVectorizer(ngram_range=(2, 4), analyzer='char')
X = vectorizer.fit_transform(ngram_df['mask'])
for mask in vectorizer.get_feature_names()[0:10]:
  print(mask)

# COMMAND ----------

# MAGIC %md
# MAGIC With numbers of clusters set to 3, we aim at detecting structured, unstructured and semi structured data, all driven by different mask characteristics. For example, a mask such as `AW9AW9A9W` would be indicative of a table of numbers. Please note that different content will yield different outputs requiring an analyst / scientist / engineer to tweak this notebook accordingly.

# COMMAND ----------

from sklearn.cluster import KMeans
import numpy as np
kmeans = KMeans(n_clusters=k, random_state=0).fit(X)
ys = kmeans.transform(X)
ngram_df['cluster'] = [np.argmin(y) for y in ys]
ngram_df['distance'] = [np.min(y) for y in ys]

# COMMAND ----------

# MAGIC %md
# MAGIC Let's find the most descriptive documents for each identified cluster. As expected, we could find pages of highly unstructured text as well a tabular information. In this example, we will consider `cluster_1` to be made of highly structured information that we may want to delegate to a post-processing layer such as AWS Textract. `cluster_2`, however, contains plain text content that was already extracted through Tika with little or no benefits (and high costs) for a post processing layer.  

# COMMAND ----------

from io import BytesIO
from pdf2image import convert_from_bytes
import matplotlib.pyplot as plt

descriptive_docs = ngram_df.loc[ngram_df.groupby('cluster')['distance'].idxmin()].reset_index(drop=True)
for cid in range(k):
    rec = descriptive_docs[descriptive_docs['cluster'] == cid].reset_index().iloc[0]
    img = convert_from_bytes(rec.content)[0]
    plt.figure(figsize=(10,10))
    plt.title('cluster {}'.format(cid))
    plt.xticks([], [])
    plt.yticks([], [])
    plt.imshow(img)

# COMMAND ----------

# MAGIC %md
# MAGIC Once again, this apparent simple approach to mask based profiling seems to yield powerful insights, successfully separating 3 different structures of documents. The next step will be to move towards a supervised learning approach and automatically recognize documents as Text vs Table as new information unfold (as new files are dropped to cloud storage such as S3)

# COMMAND ----------

# MAGIC %md
# MAGIC # Classifying documents
# MAGIC Given the intel we learned through an unsupervised learning approach, we create a supervised learning model that can explicitly search for tabular information. The model used here (Naive Bayes) was built for demonstration purpose, and, despite of its apparent high accuracy, was trained against a few records only. Please make sure to adopt an appropriate model strategy for your data, type, volume and complexity. 

# COMMAND ----------

from sklearn.utils import resample
import pandas as pd

# let's separate table content from text content
tb_df = ngram_df[ngram_df['cluster'] == 1].copy().reset_index()
tx_df = ngram_df[ngram_df['cluster'] == 2].copy().reset_index()

# this becomes our target variable
tb_df['label'] = 1
tx_df['label'] = 0

dfs = [tb_df, tx_df]
majority = np.min([df.shape[0] for df in dfs])

def sample(df, n):
  return resample(
    df, 
    replace=True,                  # sample with replacement
    n_samples=n,                   # to match majority class
    random_state=123               # reproducible results
  )              

# Combine minority class with downspampled majority class
dfs_sampled = [sample(df, majority) for df in dfs]
df_sampled = pd.concat(dfs_sampled)

# COMMAND ----------

from sklearn.model_selection import train_test_split
y = df_sampled['label']
X = df_sampled['mask']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# COMMAND ----------

from sklearn.pipeline import Pipeline
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import accuracy_score, confusion_matrix
import mlflow

with mlflow.start_run(run_name='table_classifier'):
  run_id = mlflow.active_run().info.run_id

  # Naive Bayes classifier
  classifier = MultinomialNB()

  # define pipeline
  pipeline = Pipeline([
    ('vectorizer', vectorizer),
    ('classifier', classifier)
  ])

  # Train pipeline
  pipeline.fit(X_train, y_train)  
  y_pred = pipeline.predict(X_test)
  accuracy = accuracy_score(y_pred, y_test)
  
  # Log pipeline to mlflow
  mlflow.sklearn.log_model(pipeline, "pipeline")
  mlflow.log_metric("accuracy", accuracy)

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# create confusion matrix
pred_df = pd.DataFrame(zip(pipeline.predict(X_test), y_test), columns=['predicted', 'actual'])
confusion_matrix = pd.crosstab(pred_df['actual'], pred_df['predicted'], rownames=['label'], colnames=['prediction'])

# plot confusion matrix
plt.figure(figsize=(6,5))
sns.heatmap(confusion_matrix, annot=True, cmap="Blues", fmt='d')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC As previously mentioned, our model achieved perfect accuracy but was validated against only 8 records! Our goal was not to build the perfect model but to demonstrate how different organizations could use a similar approach in their digitization journeys. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registering model
# MAGIC In our previous section, we demonstrated that a model could pick on structural differences in documents to isolate tables from text. Using MLFlow and its [PyFunc](https://www.mlflow.org/docs/latest/python_api/mlflow.pyfunc.html) support, we can embed this business logic (including mask based profiling), term frequency analysis and model to be used independently of this notebook, on batch, on stream, or behind an API, together with all its required dependencies. 

# COMMAND ----------

class PyfuncClassifier(mlflow.pyfunc.PythonModel):
  
  import re
  import unidecode
  
  def _mask(self, text):
    text = text.lower()
    text = unidecode.unidecode(text)
    text = re.sub('[\s\t\n]+', 'S', text)
    text = re.sub('[a-z]+', 'A', text)
    text = re.sub('[0-9]+', '9', text)
    text = re.sub('[^AS9]+', 'W', text)
    return text
  
  def __init__(self, pipeline):
    self.pipeline = pipeline
    
  def predict(self, context, xs):
    # We expect a single column dataframe as an input as we will be using as a UDF
    X = xs[xs.columns[0]].map(lambda x: self._mask(x))
    y = self.pipeline.predict(X)
    return y
  
# We ensure that pyfunc has registered sklearn as dependency
conda_env = mlflow.pyfunc.get_default_conda_env()
conda_env['dependencies'][2]['pip'] += ['scikit-learn']
conda_env['dependencies'][2]['pip'] += ['unidecode']

# COMMAND ----------

with mlflow.start_run(run_name=model_name):
  mlflow.pyfunc.log_model('pipeline', python_model=PyfuncClassifier(pipeline), conda_env=conda_env)
  pyfunc_run_id = mlflow.active_run().info.run_id

# COMMAND ----------

client = mlflow.tracking.MlflowClient()
result = mlflow.register_model("runs:/{}/pipeline".format(pyfunc_run_id), model_name)
version = result.version

# COMMAND ----------

# MAGIC %md
# MAGIC Ideally, our model would need to go through different reviews before transitioning to production and executed against live feeds. Let's consider it final and transition it to production stage, programmatically.

# COMMAND ----------

# archive previous version
for model in client.search_model_versions("name='{}'".format(model_name)):
  if model.current_stage == 'Production':
    client.transition_model_version_stage(name=model_name, version=int(model.version), stage="Archived")
    
# transition this model to production
client.transition_model_version_stage(name=model_name, version=version, stage="Production")

# COMMAND ----------

# MAGIC %md
# MAGIC We can easily validate our approach with a test example. The command below should be able to recognize these ESG insights from CARLSBERG GROUP as tabular information. 

# COMMAND ----------

model = mlflow.pyfunc.load_model('models:/{}/production'.format(model_name))
test_df = pd.DataFrame(['CARLSBERG GROUP SUSTAINABILITY REPORT 2020   GOVERNANCE AND TRANSPARENCY 67   DATA SUMMARY TABLE  ENERGY, CARBON AND WATER          2015 2016 2017 2018 2019 2020            General production figures           Number of reporting sites 110  92 85 85 82 88 Beer production (million hl) 105.4 100.9 97.9 102.2 101.4 100.2 Soft drinks production (million hl) 13.9 14.3 14.2 15.6 16.1 16.5 Total production of beer and soft drinks (million hl)* 119.3 115.2 112.1 117.8 117'], columns=['text'])
assert(model.predict(test_df)[0] == 1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering content for postprocessing
# MAGIC With our model registered as a production artifact, we can easily embed its logic as a spark dataframe. Evaluated against live feeds, we isolate tables from plain text records. While the latter was already extracted out of the box through our Tika input format, the former will be delegated to a more expensive and time consuming business logic, whether in-house, proprietary (e.g. John Snow Labs) or using cloud native technologies such as AWS textract. 

# COMMAND ----------

# loading our model as a user defined function
contains_table_udf = mlflow.pyfunc.spark_udf(spark, 'models:/{}/production'.format(model_name))

# COMMAND ----------

from pyspark.sql import functions as F

# model inference, filtering out content that include tabular information
table_df = spark \
    .read \
    .format('tika') \
    .load(landing_zone_fs) \
    .filter(contains_table_udf(F.col('contentText')) == 1) \
    .select('path', 'contentText')

display(table_df)

# COMMAND ----------

# MAGIC %md
# MAGIC As a side note, fast forward 2 years since our original study on ESG investing (see [solution](https://www.databricks.com/solutions/accelerators/esg)), we still observe less than 5% of our CSR reports to be actual tables and facts, 95% of data being text, initiatives and aspirations. 

# COMMAND ----------

# MAGIC %md
# MAGIC Since we stored each page individually with original document uniquely identified, we can leverage that information to optimize calls to our post processing logic. 

# COMMAND ----------

@udf('string')
def extract_uuid(name):
  return name.split('/')[-3]

grouped_payloads_df = table_df \
    .groupBy(extract_uuid('path').alias('uuid')) \
    .agg(F.collect_list('path').alias('files'))

display(grouped_payloads_df)

# COMMAND ----------

# MAGIC %md
# MAGIC By only delegating < 5% of our digitized documents to post-processing tools such as AWS Textract, Azure Cognitive Services for OCR or GCP's Cloud Vision API, we are in a position to better control operation costs and data throughput, results being asychronously stored back to cloud storage as JSON records (out of scope). On the other hand, we demonstrated how 95% of information could have been digitalized out of the box with no need for complex processing engine or expensive solutions. As mentioned in the introduction, although we used an example of PDF documents here, Tika is a powerful toolkit that may soon become your companion library in your digitalization of financial documents journey.
