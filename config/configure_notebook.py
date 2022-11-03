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
s3_bucket = 'db-industry-gtm'
landing_zone = 'fsi/datasets/digitization/csr/files'
landing_zone_fs = 's3://{}/{}/**/pages'.format(s3_bucket, landing_zone)
model_name = 'table_classification'
k = 3
