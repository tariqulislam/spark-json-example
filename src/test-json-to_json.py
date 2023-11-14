from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

class CreateJsonField:

    def test_to_json_func(self):
        spark = SparkSession.builder\
             .master("spark://localhost:7077")\
             .appName("CreateJsonFormatField")\
             .getOrCreate()
        
        df=spark.read.format('csv')\
            .option("inferSchema", True)\
            .option('header', True)\
            .load("file:///home/hadoop/spark-json-example/data/modified_movie.csv")
        df.show()
        df.printSchema()

        schema = T.StructType(
            [
                T.StructField('title', T.StringType(), True),
                T.StructField('rating', T.StringType(), True),
                T.StructField('releaseYear', T.StringType(), True),
                T.StructField('genre', T.StringType(), True)
            ]
        )

        df2=df.select(F.col("id"), F.to_json(\
         F.struct("title",\
                 "rating", \
                 "releaseYear", \
                 "genre"))\
                .alias("movie"))
        df2.printSchema()
        df2.show()

        

if __name__ == "__main__":
    cjf = CreateJsonField()
    cjf.test_to_json_func()