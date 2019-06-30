from __future__ import print_function
import config
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

    # ---WRITE CUSTOM SCHEMA TO OVERRIDE SCHEMA INFERENCE---

    # create custom schema for blockchain data
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

    # ---READ IN JSON DATA FROM S3 AND PROCESS---

    # read in JSON files from S3 into DataFrame
    json_df = spark.read.json(config.SPARK_CONFIG['S3_JSON'], multiLine=True, schema=schema) \
        .withColumn("tx", explode("tx"))

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

    # write out to PostgreSQL
    write_tx_to_pg(tx_df)


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


def write_tx_to_pg(df):
    """
    Write out to PostgreSQL
    Based on EC2 Public DNS, database, and table name
    """
    df.write.mode("append")\
        .jdbc(config.SPARK_CONFIG['PG_URL'] + config.SPARK_CONFIG['PG_PORT'] + "/" + config.SPARK_CONFIG['PG_DB'], config.SPARK_CONFIG['PG_TX_TABLE'],
              properties={"user": config.SPARK_CONFIG['PG_USER'], "password": config.SPARK_CONFIG['PG_PASSWORD']})


if __name__ == "__main__":
    """
    Setup Spark session
    """
    # set up spark context and session
    spark_context = SparkContext(conf=SparkConf().setAppName("Transaction-JSON-Parser"))
    spark = SparkSession.builder.appName("Transaction-JSON-Parser").getOrCreate()
    spark_context = spark.sparkContext

    # run the main insertion function
    main(spark_context)

    # stop spark session
    spark.stop()

