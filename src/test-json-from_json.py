from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

APP_DIR="/home/hadoop/spark-json-example"

class ReadCSVJson:

    def read(self):
        # Create the Spark Session to handle the spark Operationn
        # Using spark "spark://localhost:7707" as master to handle 
        # Resource and operation
        spark = SparkSession.builder\
             .master("spark://localhost:7077")\
             .appName("ReadCSVJsonData")\
             .getOrCreate()
        
        # Read the csv file from data folder
        # add the schema option inferSchema True, It will create schema
        # from data source
        df=spark.read.format('csv')\
            .option("inferSchema", True)\
            .option('header', True)\
            .load(f"file://{APP_DIR}/data/flim.csv")
        df.show()
        df.printSchema()

        # create the schema to read the json text field from flim
        # dataframe using struct type and struct field
        schema = T.StructType(
            [
                T.StructField('Title', T.StringType(), True),
                T.StructField('Year', T.StringType(), True),
                T.StructField('Rated', T.StringType(), True),
                T.StructField('Released', T.StringType(), True),
                T.StructField('Genre', T.StringType(), True)
            ]
        )
        
        # Using the withColumn function 
        # which transforms string json field (movie)
        # to data structure which is defined into schema variable
        movie_df_mapped = df.withColumn("movie", F.from_json("movie", schema))
        movie_df_mapped.show(truncate=False)
        movie_df_mapped.printSchema()

        # Using the select to get the specific columns to build 
        # new Data frame which will contains following below field
        movie_df = movie_df_mapped.select(F.col('id'), \
                                          F.col("movie.Title"),\
                                          F.col("movie.Year"),\
                                          F.col("movie.Rated"), \
                                          F.col("movie.Released"),\
                                          F.col("movie.Genre"))
        movie_df.show(truncate=False)
        movie_df.printSchema()

        # movie_df.write\
        # .format("csv")\
        # .option("header", True)\
        # .mode("overwrite")\
        # .csv(f"file://{APP_DIR}/data/modified-movie.csv")
        

if __name__ == "__main__":
    rcj = ReadCSVJson()
    rcj.read()