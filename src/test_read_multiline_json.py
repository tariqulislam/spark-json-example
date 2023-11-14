from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


class ReadMultilineJson:
    
    def test_read_multiline_json(self):
        spark = SparkSession.builder\
             .master("spark://localhost:7077")\
             .appName("CreateJsonFormatField")\
             .getOrCreate()
        
        df = spark.read\
        .option("multiline", True) \
        .option("inferSchema", True)\
        .json("file:///home/hadoop/spark-json-example/data/multiline-json.json")

        df.printSchema()
        df.show()



if __name__ == '__main__':
    read_multiline_json = ReadMultilineJson()
    read_multiline_json.test_read_multiline_json()