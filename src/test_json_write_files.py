from pyspark.sql import SparkSession, \
    functions as F, \
        types as T

APP_DIR="/home/hadoop/spark-json-example"

class WriteJsonFile:
    def test_write_json_file(self):
        # Create the Spark Session to handle the spark Operationn
        # Using spark "spark://localhost:7707" as master to handle 
        # Resource and operation
        spark = SparkSession.builder\
            .master("spark://localhost:7077")\
            .appName("CreateJsonFormatField")\
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

        # Create the New Data frame to generate the Json file by 
        # Select function
        movie_df = movie_df_mapped.select(F.col('id'), \
                                         F.col("movie.Title"),\
                                         F.col("movie.Year"),\
                                         F.col("movie.Rated"),\
                                         F.col("movie.Released"),\
                                         F.col("movie.Genre"))
        movie_df.show(truncate=False)
        movie_df.printSchema()
        # using the spark Data frame write function
        # with mode is overwrite (which will replace the data everytime
        # write the datasource into specific location
        # Json write will by default take english format datetime
        # we have to specifiy the specify the specific datetime 
        # format is the data frame has datetime field
        movie_df.write\
        .mode("overwrite")\
        .option("dateFormat", "MM/dd/yyyy")\
       .json(f"file://{APP_DIR}/data/modified-movie")

if __name__ == '__main__':
  write_json_file = WriteJsonFile()
  write_json_file.test_write_json_file()