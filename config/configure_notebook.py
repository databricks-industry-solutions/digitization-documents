# Databricks notebook source
# MAGIC %pip install -r requirements.txt

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

# MAGIC %sh
# MAGIC cat <<EOF > /dbfs/FileStore/solution_accelerators/digitization/init.sh
# MAGIC #!/usr/bin/env bash
# MAGIC sudo apt-get install -y tesseract-ocr
# MAGIC sudo apt-get install -y poppler-utils
# MAGIC EOF

# COMMAND ----------

sector = 'i22' # Brewing companies
landing_zone = '/tmp/fsi/datasets/digitization/csr/files'
model_name = 'table_classification'
k = 3
landing_zone_fs = '{}/**/pages'.format(landing_zone)
s3_bucket = 'db-industry-gtm'
s3_landing_zone = 's3://{}{}'.format(s3_bucket, landing_zone)

# COMMAND ----------

# Set mlflow experiment explicitly to make sure the code runs in both interactive execution and job execution
import mlflow
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment('/Users/{}/document_digitization'.format(username))

# COMMAND ----------


