from __future__ import print_function
import config
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import explode, concat, col, lit, split, translate, row_number, arrays_zip
from pyspark.sql.types import *
from pyspark.sql.window import Window
from graphframes import *


# main function
def main(sc):
    """
    Main processing function
    Read in data from PostgreSQL transaction table
    Perform reverse lookup for vin transactions and return input Bitcoin values and addresses
    Perform disjoint set (i.e., union find) algorithm using GraphFrames
    Write out address clustering results to PostgreSQL
    """

    # ---READ IN TRANSACTION DATA AND PERFORM REVERSE TX LOOKUP USING JOINS---

    # create initial SQL query
    # tx_query = "SELECT txid, height, time, ntx, vin_coinbase, vin_txid, vin_vout, vout_value, vout_n, vout_addresses FROM {} WHERE height <= 400000 LIMIT 5000000"\
    tx_query = "SELECT txid, height, time, ntx, vin_coinbase, vin_txid, vin_vout, vout_value, vout_n, vout_addresses FROM {}"\
        .format(config.SPARK_CONFIG['PG_TX_TABLE'])

    # read in data from PostgreSQL
    tx_df = spark.read \
        .format(config.SPARK_CONFIG['PG_FORMAT']) \
        .option("url", config.SPARK_CONFIG['PG_URL'] + config.SPARK_CONFIG['PG_PORT'] + "/" + config.SPARK_CONFIG['PG_DB']) \
        .option("user", config.SPARK_CONFIG['PG_USER']) \
        .option("password", config.SPARK_CONFIG['PG_PASSWORD'])\
        .option("query", tx_query) \
        .option("numPartitions", '10000') \
        .load()
    # display_df(tx_df)

    # select priority columns, convert array columns, and zip vin and vout fields
    clean_df = tx_df.withColumn("vin_txid_arr", split(col("vin_txid"), ",\s*")) \
        .withColumn("vin_vout_arr", split(col("vin_vout"), ",\s*")) \
        .withColumn("vin_txid_vout_zip", arrays_zip("vin_txid_arr", "vin_vout_arr")) \
        .withColumn("vout_value_arr", split(col("vout_value"), ",\s*")) \
        .withColumn("vout_n_arr", split(col("vout_n"), ",\s*")) \
        .withColumn("vout_addresses_arr", split(col("vout_addresses"), ",\s*")) \
        .withColumn("vout_value_n_addr_zip", arrays_zip("vout_value_arr", "vout_n_arr", "vout_addresses_arr"))
    # display_df(clean_df)

    # # create left side DataFrame
    vin_cols = ['txid', 'height', 'time', 'ntx', 'vin_coinbase', 'vin_txid_vout_zip']
    vin_df = clean_df.select(vin_cols) \
        .withColumn("vin_txid_vout_tup", explode("vin_txid_vout_zip")) \
        .withColumn("vin_txid", col("vin_txid_vout_tup").vin_txid_arr) \
        .withColumn("vin_vout", col("vin_txid_vout_tup").vin_vout_arr) \
        .drop("vin_txid_vout_zip") \
        .drop("vin_txid_vout_tup") \
        .withColumn("left_key", concat(col("vin_txid"), lit("-"), col("vin_vout")))
    # display_df(vin_df)

    # create right side DataFrame
    vout_cols = ['txid', 'vout_value_n_addr_zip']
    vout_df = clean_df.select(vout_cols) \
        .withColumn("vout_value_n_addr_tup", explode("vout_value_n_addr_zip")) \
        .withColumn("vout_value", col("vout_value_n_addr_tup").vout_value_arr) \
        .withColumn("vout_n", col("vout_value_n_addr_tup").vout_n_arr) \
        .withColumn("vout_addr_pre", col("vout_value_n_addr_tup").vout_addresses_arr) \
        .withColumn("vout_addr", translate(col("vout_addr_pre"), '[]', '')) \
        .drop("vout_value_n_addr_zip") \
        .drop("vout_value_n_addr_tup") \
        .drop("vout_addr_pre") \
        .withColumnRenamed("txid", "txid2") \
        .withColumn("right_key", concat(col("txid2"), lit("-"), col("vout_n"))) \
        .drop("txid2")
    # display_df(vout_df)

    # join DataFrames
    join_df = vin_df.join(vout_df, vin_df.left_key == vout_df.right_key, 'left') \
        .drop("left_key") \
        .drop("right_key")
    # display_df(join_df)

    # create temporary table for GraphFrames
    join_df.registerTempTable("join_result")

    # ---CREATING GRAPHFRAME FOR CONNECTED COMPONENTS ALGORITHM---

    # create vertices DataFrame
    vertices = spark.sql("SELECT DISTINCT(vout_addr) FROM join_result").withColumnRenamed("vout_addr", "id")

    # generate DataFrame with single address connection for all addresses in a given txid group
    w = Window.partitionBy("txid").orderBy("vout_addr")
    first_by_txid_df = join_df.withColumn("rn", row_number().over(w)).where(col("rn") == 1) \
        .withColumnRenamed("txid", "txid2") \
        .withColumnRenamed("vout_addr", "vout_addr_first") \
        .drop("rn") \
        .drop("height")
    # first_by_txid_df.show(100)

    # join DataFrames
    interim_df = join_df.join(first_by_txid_df, join_df.txid == first_by_txid_df.txid2, 'left')

    # create edges DataFrame
    edges = interim_df.select("vout_addr", "vout_addr_first") \
        .withColumnRenamed("vout_addr", "src") \
        .withColumnRenamed("vout_addr_first", "dst") \
        .na.drop()

    # create GraphFrame
    g = GraphFrame(vertices, edges)

    # set checkpoint directory in S3
    sc.setCheckpointDir(config.SPARK_CONFIG['S3_CHECKPOINT'])

    # run connected components
    clst_result = g.connectedComponents()
    clst_result.show(100, truncate=False)

    # # ---FOR TESTING ONLY--- show result DataFrame for a specific block to verify correct results
    # clst_result.registerTempTable("clst_table")
    # view_df = spark.sql("SELECT * FROM clst_table ORDER BY clst_table.component")
    # view_df.show(1000, truncate=False)

    # write out to PostgreSQL
    write_clst_to_pg(clst_result)


def display_df(df):
    """
    Quality of life function
    Prints schema and tabular view for a DataFrame
    """
    df.printSchema()
    df.show(2000, truncate=False)


def display_col(df, col):
    """
    Quality of life function
    Prints tabular view of a single column for a DataFrame
    """
    df.select(col).show(truncate=False)


def write_clst_to_pg(df):
    """
    Write out to PostgreSQL
    Based on EC2 Public DNS, database, and table name
    """
    df.write.mode("append")\
        .jdbc(config.SPARK_CONFIG['PG_URL'] + config.SPARK_CONFIG['PG_PORT'] + "/" + config.SPARK_CONFIG['PG_DB'], config.SPARK_CONFIG['PG_CLST_TABLE'],
              properties={"user": config.SPARK_CONFIG['PG_USER'], "password": config.SPARK_CONFIG['PG_PASSWORD']})


if __name__ == "__main__":
    """
    Setup Spark session
    """
    # set up spark context and session
    spark_context = SparkContext(conf=SparkConf().setAppName("Tx-Reverse-Lookup"))
    spark = SparkSession.builder.appName("Tx-Reverse-Lookup").getOrCreate()
    spark_context = spark.sparkContext

    # run the main insertion function
    main(spark_context)

    # stop spark session
    spark.stop()

