from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

class TestGetJsonTextExtract:

     # extract the json String
     def test_get_json_object_tuple_func(self):
        spark = SparkSession.builder\
             .master("spark://localhost:7077")\
             .appName("CreateJsonFormatField")\
             .getOrCreate()
        
        df=spark.read.format('csv')\
            .option("inferSchema", True)\
            .option('header', True)\
            .load(\
         "file:///home/hadoop/spark-json-example/data/flim.csv")
        df.show()
        df.printSchema()

      
        
        df2 = df.select(
                F.col("id"),
                F.get_json_object(F.col("movie"), "$.title").alias("title"),
                F.get_json_object(F.col("movie"), "$.rating").alias("rating")
        )
        df2.show()
        df2.printSchema()

        # Json Tuple function
        df3 = df.select(F.col("id"),
                         F.json_tuple(F.col("movie"), "title", "rating"))
        df3.show()
        df3.printSchema()

if __name__ == '__main__':
   test_class = TestGetJsonTextExtract()
   test_class.test_get_json_object_tuple_func()