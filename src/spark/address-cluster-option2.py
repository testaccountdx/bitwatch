# copy file using scp -i ~/.ssh/mycelias-IAM-keypair.pem address-cluster1.py XX.compute-1.amazonaws.com:/home/ec2-user/spark-scripts
# run using $SPARK_HOME/bin/spark-submit address-cluster1.py

#***OPTION #2: EXPLODE METHOD***

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
                multisig = "|".join([str(x) for x in val])
                result.append(multisig)

        return result

    # TEST CONVERTER UDF
    convert_udf = udf(lambda x: array_of_arrays_to_string(x), StringType())

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
        .withColumn("vin_coinbase", concat_ws(",", df_test1.tx.vin.coinbase))\
        .withColumn("vin_txid", concat_ws(",", df_test1.tx.vin.txid))\
        .withColumn("vin_vout", concat_ws(",", df_test1.tx.vin.vout))\
        .withColumn("vout_value", concat_ws(",", df_test1.tx.vout.value))\
        .withColumn("vout_n", concat_ws(",", df_test1.tx.vout.n))\
        .withColumn("vout_addresses_pre", df_test1.tx.vout.scriptPubKey.addresses)\
        .withColumn("vout_addresses", convert_udf("vout_addresses_pre"))\
        .drop("tx")\
        .drop("vout_addresses_pre")

    # show DataFrame
    df_test2.show()

    # print schema
    df_test2.printSchema()

    # drop unnecessary columns
    ###drop_cols = ["", "", "", "", ""]
    ###df_test2 = df_test2.drop()

    # show specific column (TESTING PURPOSES ONLY)
    df_test2.select("vout_addresses").show(truncate=False)

    df_test2.write \
        .mode("append")\
        .jdbc("jdbc:postgresql://ec2-18-209-241-29.compute-1.amazonaws.com:5432/mycelias", "transactions4",
              properties={"user": "postgres", "password": "postgres"})

    spark.stop()
