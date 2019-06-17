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
        .jdbc("jdbc:postgresql://ec2-18-209-241-29.compute-1.amazonaws.com:5432/postgres", "demo",
              properties={"user": "postgres", "password": "postgres"})

    # print schema
    test_df.printSchema()

    test_df.show()

    spark.stop()
