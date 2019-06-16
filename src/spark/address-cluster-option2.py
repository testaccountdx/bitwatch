# copy file using scp -i ~/.ssh/mycelias-IAM-keypair.pem address-cluster1.py XX.compute-1.amazonaws.com:/home/ec2-user/spark-scripts
# run using $SPARK_HOME/bin/spark-submit address-cluster1.py

#***OPTION #2: EXPLODE METHOD***

from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.functions import explode

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
    df = spark.read.json(path, multiLine=True)
    df.show()

    # print schema
    df.printSchema()

    # test explode function at tx level
    df_test1 = df.withColumn("tx", explode(df.tx))
    df_test1.show()

    # print schema
    df_test1.printSchema()

    # test adding new column at the tx level
    df_test2 = df_test1.withColumn("txid", df_test1.tx.txid)\
        .withColumn("vin_coinbase", df_test1.tx.vin.coinbase)\
        .withColumn("vin_txid", df_test1.tx.vin.txid)\
        .withColumn("vin_vout", df_test1.tx.vin.vout)\
        .withColumn("vout_value", df_test1.tx.vout.value)\
        .withColumn("vout_n", df_test1.tx.vout.n)\
        .withColumn("vout_addresses", df_test1.tx.vout.scriptPubKey.addresses)

    # show DataFrame
    df_test2.show()

    # print schema
    df_test2.printSchema()

    # drop unnecessary columns
    drop_cols = ["", "", "", "", ""]
    df_test2 = df_test2.drop()

    # show specific column (JUST FOR TESTING)
    ###df_test2.select("vout_addresses").show(truncate=False)

    spark.stop()
