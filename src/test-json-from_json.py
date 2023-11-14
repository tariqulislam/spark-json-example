from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

class ReadCSVJson:

    def read(self):
        spark = SparkSession.builder\
             .master("spark://localhost:7077")\
             .appName("ReadCSVJsonData")\
             .getOrCreate()
        
        df=spark.read.format('csv')\
            .option("inferSchema", True)\
            .option('header', True)\
            .load("file:///home/hadoop/spark-json-example/data/flim.csv")
        df.show()
        df.printSchema()

        # create the schema
        schema = T.StructType(
            [
                T.StructField('title', T.StringType(), True),
                T.StructField('rating', T.StringType(), True),
                T.StructField('releaseYear', T.StringType(), True),
                T.StructField('genre', T.StringType(), True)
            ]
        )

        movie_df_mapped = df.withColumn("movie", F.from_json("movie", schema))
        movie_df_mapped.show(truncate=False)
        movie_df_mapped.printSchema()

        movie_df = movie_df_mapped.select(F.col('id'), \
                                         F.col("movie.title"),\
                                          F.col("movie.rating"),\
                                            F.col("movie.releaseYear"), \
                                              F.col("movie.genre"))
        movie_df.show(truncate=False)
        movie_df.printSchema()

if __name__ == "__main__":
    rcj = ReadCSVJson()
    rcj.read()