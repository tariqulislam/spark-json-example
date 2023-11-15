from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

APP_DIR="/home/hadoop/spark-json-example"

class CreateJsonField:

    def test_to_json_func(self):
        # Create the Spark Session to handle the spark Operationn
        # Using spark "spark://localhost:7707" as master to handle 
        # Resource and operation
        spark = SparkSession.builder\
             .master("spark://localhost:7077")\
             .appName("CreateJsonField")\
             .getOrCreate()

        # data source file location 
        read_file=f"file://{APP_DIR}/data/modified-movies.csv"
        
        # Read the csv file from data folder
        # add the schema option inferSchema True, It will create schema
        # from data source
        df=spark.read.format('csv')\
            .option("inferSchema", True)\
            .option('header', True)\
            .load(read_file)
        df.show()
        df.printSchema()

  
        # Using the select 
        # and to_json function to make create the json text field
        # from different column 
        # in to new data frame
        df2=df.select(F.col("id"), F.to_json(\
         F.struct("Title",\
                 "Year", \
                 "Rated", \
                 "Released",\
                 "Genre"))\
                .alias("movie"))
        df2.printSchema()
        df2.show()

if __name__ == "__main__":
    cjf = CreateJsonField()
    cjf.test_to_json_func()