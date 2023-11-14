from pyspark.sql import SparkSession, \
    functions as F, \
        types as T


class WriteJsonFile:
    def test_write_json_file(self):
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

        movie_df.write\
        .mode("overwrite")\
        .option("dateFormat", "MM/dd/yyyy")\
       .json("file:///home/hadoop/spark-json-example/data/modified-movie")

if __name__ == '__main__':
  write_json_file = WriteJsonFile()
  write_json_file.test_write_json_file()