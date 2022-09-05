import unittest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pathlib import Path


class SparkTest(unittest.TestCase):

    def setUp(self):

        # retrieve all jar files required for test
        path = Path(os.getcwd())
        dep_path = os.path.join(path, 'build', 'dependencies')
        dep_file = [os.path.join(dep_path, f) for f in os.listdir(dep_path)]
        spark_conf = ':'.join(dep_file)
        self.spark_conf = spark_conf

        # inject scala classes
        self.spark = SparkSession.builder.appName("tika-ocr-inputformat") \
            .config("spark.driver.extraClassPath", spark_conf) \
            .master("local") \
            .getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()

    def test_text(self):
        path = Path(os.getcwd()).parent.absolute()
        files = os.path.join(path, 'src', 'test', 'resources', 'text')
        df = self.spark.read.format('tika').load(files)
        df.show()
        self.assertEqual(df.filter(F.lower('contentText').contains('tika')).count(), 11)

    def test_images(self):
        path = Path(os.getcwd()).parent.absolute()
        files = os.path.join(path, 'src', 'test', 'resources', 'images')
        df = self.spark.read.format('tika').load(files)
        df.show()
        self.assertEqual(df.filter(F.lower('contentText').contains('tika')).count(), 4)

if __name__ == '__main__':
    unittest.main()