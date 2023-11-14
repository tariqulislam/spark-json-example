from pyspark.sql import SparkSession, functions as F
from urllib.request import urlopen

class ReadRemoteJsonFile:
    def read_http_json(self):
        spark = SparkSession.builder\
             .master("spark://localhost:7077")\
             .appName("CreateJsonFormatField")\
             .getOrCreate()

        url = 'https://dummyjson.com/products'
        jsonData = urlopen(url).read().decode('utf-8')
        rdd = spark.sparkContext.parallelize([jsonData])
        df = spark.read.json(rdd)
        df.show()
        df.printSchema()
       
        # Normalized the Json
        df2 = df.withColumn("products", F.explode("products"))
        df3=df2.select("products.id","products.brand",\
                *[F.col('products.images').getItem(i)\
          .alias(f'img{i+1}') for i in range(0,len('products.images'))])
        df3.printSchema()
        df3.show()

if __name__ == '__main__':
    read_remote_json_file = ReadRemoteJsonFile()
    read_remote_json_file.read_http_json()