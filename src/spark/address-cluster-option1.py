# copy file using scp -i ~/.ssh/mycelias-IAM-keypair.pem address-cluster1.py XX:/home/ec2-user/spark-scripts
# run using $SPARK_HOME/bin/spark-submit address-cluster1.py

#***OPTION #1: SQL STYLE METHOD***

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

    # create SQL context from Spark context
    sqlContext = SQLContext(sc)

    # set file path
    path = "block150000.json"

    df = sqlContext.read.json(path, multiLine=True)

    # create temporary table view
    df.createOrReplaceTempView("block")

    # run SQL commands
    sqlContext.sql("SELECT tx.size, tx.weight FROM block").show(truncate=False)

    spark.stop()
