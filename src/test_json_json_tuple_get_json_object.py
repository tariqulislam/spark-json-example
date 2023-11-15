from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

APP_DIR="/home/hadoop/spark-json-example"

class TestGetJsonTextExtract:

     # test the get json object and json tuple function
     def test_get_json_object_tuple_func(self):
        # Create the Spark Session to handle the spark Operationn
        # Using spark "spark://localhost:7707" as master to handle 
        # Resource and operation
        spark = SparkSession.builder\
             .master("spark://localhost:7077")\
             .appName("TestGetJsonTextExtract")\
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

      
        # Using get_json_object 
        # SparkSQL function (column_name, "$.json_attribute_name")
        # Extract the different json attribute and value 
        # to new dataframe columns and value
        df2 = df.select(
             F.col("id"),
             F.get_json_object(F.col("movie"), "$.Title").alias("Title"),
             F.get_json_object(F.col("movie"), "$.Rated").alias("Rated"),
             F.get_json_object(F.col("movie"), "$.Year").alias("Year"),
             F.get_json_object(F.col("movie"), "$.Released").alias("Released")
        )
        df2.show()
        df2.printSchema()

        # Using Json Tuple function 
        # json_tuple(column_name, "json_attr_1", "json_attr_2", ....)
        # Extract the each attribute into new dataframe column
        df3 = df.select(F.col("id"),
                        F.json_tuple(F.col("movie"),
                        "Title", "Rated", "Released", "Year")
                        .alias("Title", "Rated", "Released", "Year"))
        df3.show()
        df3.printSchema()

if __name__ == '__main__':
   test_class = TestGetJsonTextExtract()
   test_class.test_get_json_object_tuple_func()