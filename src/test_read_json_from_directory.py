from pyspark.sql import SparkSession, functions as F
from urllib.request import urlopen

class ReadWriteJsonDirectory:

     def read_json_dir(self):
        spark = SparkSession.builder\
             .master("spark://localhost:7077")\
             .appName("CreateJsonFormatField")\
             .getOrCreate()

        # Usage for Map and Struct to_json 

        df_read_write_data = spark.read\
        .format("json")\
        .load("file:///home/hadoop/spark-json-example/data/modified-movie")
        df_read_write_data.printSchema()
        df_read_write_data.show()

if __name__ == '__main__':
    read_json_directory = ReadWriteJsonDirectory()
    read_json_directory.read_json_dir()