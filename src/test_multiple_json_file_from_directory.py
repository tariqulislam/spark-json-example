from pyspark.sql import SparkSession, functions as F
from urllib.request import urlopen

class ReadMultipleJsonFromDirectory:

     def read_multiple_file_from_dir(self):
        spark = SparkSession.builder\
             .master("spark://localhost:7077")\
             .appName("CreateJsonFormatField")\
             .getOrCreate()

        # Usage for Map and Struct to_json 

        df_read_json = spark.read\
        .format("json")\
        .option("inferSchema", True)\
        .option("multiline", True)\
        .load("file:///home/hadoop/spark-json-example/data/multi-json-record/*.json")
        df_read_json.printSchema()
        df_read_json.show()

if __name__ == '__main__':
    read_dir = ReadMultipleJsonFromDirectory()
    read_dir.read_multiple_file_from_dir()