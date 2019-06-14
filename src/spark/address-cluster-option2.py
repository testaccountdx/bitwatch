# copy file using scp -i ~/.ssh/mycelias-IAM-keypair.pem address-cluster1.py XX.compute-1.amazonaws.com:/home/ec2-user/spark-scripts
# run using $SPARK_HOME/bin/spark-submit address-cluster1.py

#***OPTION #2: EXPLODE METHOD***

from __future__ import print_function
import sys
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

    # create DataFrame
    df = spark.read.json(path, multiLine=True).select("tx")

    # print schema
    df.printSchema()

    df.withColumn("txid", df.tx.txid)\
        .withColumn("vout_address", explode(df.tx.vout.scriptPubKey.address))\
        .show(50)

    # below code works!
    # use explode function to deconstruct transactions into rows
    ###df.withColumn("vout", explode(df.tx.vout))\
    ###    .show()

    ##df.createOrReplaceTempView("block")

    ##addressDF = spark.sql("SELECT tx.txid, tx.hash, tx.version FROM block")
    ##addressDF.show()

    ## read in JSON file and select target field(s)
    #df = sqlc.read.json("block0.json", multiLine=True).select("hash")

    ## show first element
    #df.show(1, False)

    spark.stop()
