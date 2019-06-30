from __future__ import print_function
import sys
import os
from pyspark import SparkContext, SparkConf, sql
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import explode, concat_ws, udf, concat, col, lit, when, split, translate
from pyspark.sql.types import *
from graphframes import *


# main parsing function
def main(sc):
    """
    Main processing function
    Read in data from PostgreSQL transaction table
    Perform reverse lookup for vin transactions and return Bitcoin values and addresses
    Write out results on a per transaction basis to PostgreSQL
    """
    # pass in AWS keys
    # sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    # sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])

    # define S3 bucket location for storing checkpoints
    path = "s3a://bitcoin-checkpoint/"

    # read in JSON files into DataFrame
    # json_df = spark.read.json(path, multiLine=True) \
    #     .withColumn("tx", explode("tx"))

    # json_df.show()

    print("---STARTING SPARK JOB---")

    vertices = spark.createDataFrame([('1', 'Carter'),
                                      ('2', 'May'),
                                      ('3', 'Mills'),
                                      ('4', 'Hood'),
                                      ('5', 'Banks'),
                                      ('98', 'Berg'),
                                      ('99', 'Page')],
                                     ['id', 'name'])

    edges = spark.createDataFrame([('1', '2'),
                                   ('2', '1'),
                                   ('3', '1'),
                                   ('1', '3'),
                                   ('2', '3'),
                                   ('3', '4'),
                                   ('4', '3'),
                                   ('5', '3'),
                                   ('3', '5'),
                                   ('4', '5'),
                                   ('98', '99'),
                                   ('99', '98')],
                                  ['src', 'dst'])

    g = GraphFrame(vertices, edges)

    # g.vertices.show()
    # g.edges.show()

    # g.degrees.show()

    # set checkpoint directory ---IMPORTANT---
    sc.setCheckpointDir(path)

    # run connected components
    g.connectedComponents().show(200)


# def vin_zip(x, y):
#     """
#     UDF function
#     Zips vin columns element-wise
#     """
#     return list(zip(x, y))
#
#
# def vout_zip(x, y, z):
#     """
#     UDF function
#     Zips vin columns element-wise
#     """
#     return list(zip(x, y, z))
#
#
# def display_df(df):
#     """
#     Quality of life function
#     Prints schema and tabular view for a DataFrame
#     """
#     df.printSchema()
#     df.show(100, truncate=True)
#
#
# def display_col(df, col):
#     """
#     Quality of life function
#     Prints tabular view of a single column for a DataFrame
#     """
#     df.select(col).show(truncate=True)


# def write_to_postgres(df):
#     """
#     Write out to PostgreSQL
#     Based on EC2 Public DNS, database, and table name
#     """
#     df.write.mode("append")\
#         .jdbc("jdbc:postgresql://ec2-34-204-179-83.compute-1.amazonaws.com:5432/mycelias", "transactions",
#               properties={"user": "postgres", "password": "postgres"})


if __name__ == "__main__":
    """
    Setup Spark session and AWS, postgres access keys
    """
    spark_context = SparkContext(conf=SparkConf().setAppName("Disjoint-Set"))
    # os.environ["AWS_ACCESS_KEY_ID"] = sys.argv[1]
    # os.environ["AWS_SECRET_ACCESS_KEY"] = sys.argv[2]
    # os.environ["AWS_DEFAULT_REGION"] = sys.argv[3]
    # os.environ["POSTGRES_URL"] = sys.argv[4]
    # os.environ["POSTGRES_USER"] = sys.argv[5]
    # os.environ["POSTGRES_PASSWORD"] = sys.argv[6]

    # create spark session
    spark = SparkSession.builder.appName("Disjoint-Set").getOrCreate()
    spark_context = spark.sparkContext

    # run the main insertion function
    main(spark_context)

    # stop spark session
    spark.stop()

