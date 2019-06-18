# copy file using scp -i ~/.ssh/mycelias-IAM-keypair.pem address-cluster1.py XX.compute-1.amazonaws.com:/home/ec2-user/spark-scripts
# run using $SPARK_HOME/bin/spark-submit address-cluster1.py

from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.functions import explode, concat_ws, udf
from pyspark.sql.types import StringType

if __name__ == "__main__":
    """
        XX
    """

    # function for converting addresses in vout to array of strings (delimited by pipe character for multisig)
    def array_of_arrays_to_string(x):
        result = []
        for val in x:
            if len(val) == 1:
                result.append(str(val[0]))
            else:
                multisig = " | ".join([str(x) for x in val])
                result.append(multisig)

        return result

    # convert function to UDF
    convert_udf = udf(lambda x: array_of_arrays_to_string(x), StringType())

    # create Spark session
    spark = SparkSession.builder.appName("SparkJsonParse").getOrCreate()

    # create Spark context
    sc = spark.sparkContext

    # set file path ***EVENTUALLY WRITE LOOP OVER LARGE NUMBER OF BLOCKS***
    path = "block150000_test.json"

    # SPARK .JSON CAN READ IN * S3 PASS IN ALL USING WILDCARD CHARACTER - USE S3 URL

    # create DataFrame
    df = spark.read.json(path, multiLine=True) \
        .withColumn("tx", explode("tx"))

    # print schema and show DataFrame
    # df.printSchema()
    # df.show()

    # manipulate DataFrame to return specific columns
    tx_df = df.withColumn("txid", df.tx.txid)\
        .withColumn("vin_coinbase", concat_ws(",", df.tx.vin.coinbase))\
        .withColumn("vin_txid", concat_ws(",", df.tx.vin.txid))\
        .withColumn("vin_vout", concat_ws(",", df.tx.vin.vout))\
        .withColumn("vout_value", concat_ws(",", df.tx.vout.value))\
        .withColumn("vout_n", concat_ws(",", df.tx.vout.n))\
        .withColumn("vout_addresses_pre", df.tx.vout.scriptPubKey.addresses)\
        .withColumn("vout_addresses", convert_udf("vout_addresses_pre"))\
        .drop("tx")\
        .drop("vout_addresses_pre")

    # print schema and show DataFrame
    # df_test1.printSchema()
    # df_test1.show()

    # drop unnecessary columns
    ###drop_cols = ["", "", "", "", ""]
    ###df_test2 = df_test2.drop()

    # show specific column (TESTING PURPOSES ONLY)
    tx_df.select("vout_addresses").show(truncate=False)

    # WRITE SCRIPT PUNCH IT IN SQL TO WRITE THE SCHEMA

    # WATCH OUT FOR MEMORY PROBLEMS

    # IF YOU USE SSH THEN YOU MAY NOT HAVE TO USE NOHUP
    # RUN YOUR JOBS WITH SSH AND RUN SPARK-SUBMIT

    tx_df.write \
        .mode("append")\
        .jdbc("jdbc:postgresql://ec2-18-209-241-29.compute-1.amazonaws.com:5432/mycelias", "transactions",
              properties={"user": "postgres", "password": "postgres"})

    spark.stop()
