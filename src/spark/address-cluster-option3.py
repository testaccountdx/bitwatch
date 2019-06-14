# copy file using scp -i ~/.ssh/mycelias-IAM-keypair.pem address-cluster1.py XX:/home/ec2-user/spark-scripts
# run using $SPARK_HOME/bin/spark-submit address-cluster1.py

#***OPTION #3: ONE JSON OBJECT RDD***

from __future__ import print_function
import sys
import json
from random import random
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.functions import explode, from_json
from pyspark.sql.types import *

if __name__ == "__main__":
    """
        XX
    """

    # create Spark session
    spark = SparkSession.builder.appName("SparkJsonParse").getOrCreate()

    # create Spark context
    sc = spark.sparkContext

    # set file path ***EVENTUALLY WRITE LOOP OVER LARGE NUMBER OF BLOCKS***
    path = "block150000.json"

    with open(path) as json_file:
        data = json.load(json_file)

        # create RDD and DataFrame
        rdd = sc.parallelize(data['tx'])
        test_df = spark.read.json(rdd)

        # print schema
        test_df.printSchema()

        test_df.show()

        test_df.withColumn("vin", explode("vin")).show()

        spark.stop()
