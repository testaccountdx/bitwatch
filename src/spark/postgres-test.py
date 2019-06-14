from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import DataFrameReader

if __name__ == "__main__":
    """
        XX
    """

    # create Spark session
    spark = SparkSession.builder.appName("SparkPostgresTest").getOrCreate()

    # create Spark context
    sc = spark.sparkContext

    # create SQL context from Spark context
    sqlContext = SQLContext(sc)

    test_df = spark.read \
    .jdbc("jdbc:postgresql:XX:5432", "public.demo",
          properties={"user": "XX", "password": "XX"})

    # print schema
    test_df.printSchema()

    test_df.show()

    spark.stop()
