from __future__ import print_function
import sys
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, concat_ws, udf, concat, col, lit, when
from pyspark.sql.types import *


# main parsing function
def main(sc):
    """
    Main parsing function
    Grabs blockchain JSON files from AWS S3 bucket
    Then parses out and writes into queryable format in PostgreSQL
    """
    # pass in AWS keys
    # sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    # sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])

    # define S3 bucket location
    path = "s3a://bitcoin-json-duptest-mycelias/*"

    #test_path = "block150000_test.json"
    #test_df = spark.read.json(test_path, multiLine=True)
    #display_df(test_df)

    print("---STARTING SPARK JOB---")

    # get blockchain schema
    schema = StructType([
        StructField('bits', StringType(), True),
        StructField('chainwork', StringType(), True),
        StructField('confirmations', LongType(), True),
        StructField('difficulty', DoubleType(), True),
        StructField('hash', StringType(), True),
        StructField('height', LongType(), True),
        StructField('mediantime', LongType(), True),
        StructField('merkleroot', StringType(), True),
        StructField('nTx', LongType(), True),
        StructField('nextblockhash', StringType(), True),
        StructField('nonce', LongType(), True),
        StructField('previousblockhash', StringType(), True),
        StructField('size', LongType(), True),
        StructField('strippedsize', LongType(), True),
        StructField('time', LongType(), True),
        StructField('tx', ArrayType(
            StructType([
                StructField('hash', StringType(), True),
                StructField('hex', StringType(), True),
                StructField('locktime', LongType(), True),
                StructField('size', LongType(), True),
                StructField('txid', StringType(), True),
                StructField('version', LongType(), True),
                StructField('vin', ArrayType(
                    StructType([
                        StructField('coinbase', StringType(), True),
                        StructField('scriptSig', StructType([
                            StructField('asm', StringType(), True),
                            StructField('hex', StringType(), True),
                        ]), True),
                        StructField('sequence', LongType(), True),
                        StructField('txid', StringType(), True),
                        StructField('vout', LongType(), True),
                        StructField('txinwitness', ArrayType(
                            StringType()
                        ), True)
                    ])
                ), True),
                StructField('vout', ArrayType(
                    StructType([
                        StructField('n', LongType(), True),
                        StructField('scriptPubKey', StructType([
                            StructField('addresses', ArrayType(
                                StringType()
                            ), True),
                            StructField('asm', StringType(), True),
                            StructField('hex', StringType(), True),
                            StructField('reqSigs', LongType(), True),
                            StructField('type', StringType(), True)
                        ]), True),
                        StructField('value', DoubleType(), True)
                    ])
                ), True),
                StructField('vsize', LongType(), True),
                StructField('weight', LongType(), True)
            ])
        ), True),
        StructField('version', LongType(), True),
        StructField('versionHex', StringType(), True),
        StructField('weight', LongType(), True)
    ])

    # read in JSON files into DataFrame
    json_df = spark.read.json(path, multiLine=True, schema=schema) \
        .withColumn("tx", explode("tx"))

    print("---JSON DATA SUCCESSFULLY READ IN---")

    # prepare UDF function for processing
    convert_udf = udf(lambda x: array_of_arrays_to_string(x), StringType())

    # process DataFrame to return specific columns
    tx_df = json_df.withColumn("txid_pre", json_df.tx.txid)\
        .withColumn("txid", when((json_df.tx.txid == 'e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468') | (json_df.tx.txid == 'd5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599'), concat(col("txid_pre"), lit("-"), col("height"))).otherwise(json_df.tx.txid))\
        .withColumn("vin_coinbase", concat_ws(",", json_df.tx.vin.coinbase))\
        .withColumn("vin_txid", concat_ws(",", json_df.tx.vin.txid))\
        .withColumn("vin_vout", concat_ws(",", json_df.tx.vin.vout))\
        .withColumn("vout_value", concat_ws(",", json_df.tx.vout.value))\
        .withColumn("vout_n", concat_ws(",", json_df.tx.vout.n))\
        .withColumn("vout_addresses_pre", json_df.tx.vout.scriptPubKey.addresses)\
        .withColumn("vout_addresses", convert_udf("vout_addresses_pre"))\
        .drop("tx")\
        .drop("vout_addresses_pre")\
        .drop("nonce")\
        .drop("txid_pre")

    print("---JSON DATA SUCCESSFULLY PROCESSED INTO DATAFRAME---")

    # show schema, DataFrame and address column
    # display_df(tx_df)
    # display_col(tx_df, "vout_addresses")

    # write out to PostgreSQL
    write_to_postgres(tx_df)

    print("---JSON DATA SUCCESSFULLY WRITTEN TO POSTGRESQL---")


def array_of_arrays_to_string(x):
    """
    UDF function
    Parses single and multisig addresses
    """
    result = []
    for val in x:
        if val is not None:
            if len(val) == 1:
                result.append(str(val[0]))
            else:
                multisig = " | ".join([str(x) for x in val])
                result.append(multisig)
    return result


def display_df(df):
    """
    Quality of life function
    Prints schema and tabular view for a DataFrame
    """
    df.printSchema()
    df.show()


def display_col(df, col):
    """
    Quality of life function
    Prints tabular view of a single column for a DataFrame
    """
    df.select(col).show(truncate=False)


def write_to_postgres(df):
    """
    Write out to PostgreSQL
    Based on EC2 Public DNS, database, and table name
    """
    df.write.mode("append")\
        .jdbc("jdbc:postgresql://ec2-18-209-241-29.compute-1.amazonaws.com:5432/mycelias", "transactions",
              properties={"user": "postgres", "password": "postgres"})


if __name__ == "__main__":
    """
    Setup Spark session and AWS, postgres access keys
    """
    spark_context = SparkContext(conf=SparkConf().setAppName("Transaction-JSON-Parser"))
    # os.environ["AWS_ACCESS_KEY_ID"] = sys.argv[1]
    # os.environ["AWS_SECRET_ACCESS_KEY"] = sys.argv[2]
    # os.environ["AWS_DEFAULT_REGION"] = sys.argv[3]
    # os.environ["POSTGRES_URL"] = sys.argv[4]
    # os.environ["POSTGRES_USER"] = sys.argv[5]
    # os.environ["POSTGRES_PASSWORD"] = sys.argv[6]

    # create spark session
    spark = SparkSession.builder.appName("Transaction-JSON-Parser").getOrCreate()
    spark_context = spark.sparkContext

    # run the main insertion function
    main(spark_context)

    # stop spark session
    spark.stop()

