from udfs import geohash_udf, get_coordinate_udf

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import logging


def main() -> None:
    """
    Main ETL (Extract, Transform, Load) process for the WeatherRestaurantApp.

    This script performs the following tasks:
    1. Reads restaurant data from a CSV file, handling null coordinates by filling them using OpenCageApi information.
    2. Unions data with nulls and non-nulls, calculates geohash of coordinates, and processes the restaurant data.
    3. Reads weather data from a partitioned Parquet file.
    4. Applies geohash and drops duplicates in the weather data.
    5. Joins restaurant and weather dataframes using geohash, creating an enriched dataframe.
    6. Writes the repartitioned enriched dataframe to a Parquet file.

    :return: None
    """
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    try:
        spark = SparkSession \
            .builder \
            .master("local[*]") \
            .config("spark.app.name", "WeatherRestaurantApp") \
            .config("spark.io.compression.codec", "zstd") \
            .config("spark.sql.execution.pythonUDF.arrow.enabled", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sol.shuffle.partitions", 200) \
            .config("spark.driver.memory", "12g") \
            .config("spark.executor.memory", "12g") \
            .config("spark.executor.instances", 4) \
            .config("spark.executor.cores", 4) \
            .config("spark.sql.files.maxRecordsPerFile", 10_000) \
            .config("spark.executor.heartbeatInterval", "3600s") \
            .config("spark.network.timeout", "7200s") \
            .config("spark.network.timeoutInterval", "3600s") \
            .config("spark.driver.extraJavaOptions", "-Dlog4jspark.root.logger=WARN,console") \
            .config("spark.sql.debug.maxToStringFields", 200) \
            .enableHiveSupport() \
            .getOrCreate()

        # read data from restaurant csv file
        restaurants_df = spark.read \
            .option("header", "true") \
            .option('inferSchema', 'true') \
            .csv("data/source/restaurant_csv") \
            # .limit(100_000)

        logger.info("Read data from restaurant CSV file.")

        # filtering null data and fill it using OpenCageApi information
        null_coord_restaurant_df = restaurants_df \
            .filter(F.col('lat').isNull() | F.col('lng').isNull()) \
            .drop("lat", "lng") \
            .select("*", get_coordinate_udf(F.col('franchise_name'), F.col('city'), F.col('country')).alias("coord")) \
            .select(
                "*",
                F.col("coord.lat").alias("lat"),
                F.col("coord.lng").alias("lng")) \
            .drop("coord")

        logger.info("Filled nulls in restaurant data.")
        logger.info(f"Number of rows in null_coord_restaurant_df: {null_coord_restaurant_df.count()}")
        logger.info(f"First 10 rows in null_coord_restaurant_df:")
        null_coord_restaurant_df.show(10, truncate=False)

        # union data with nulls and non-nulls, and also get geohash of coordinates
        restaurants_df = restaurants_df \
            .filter(F.col('lat').isNotNull() & F.col('lng').isNotNull()) \
            .union(null_coord_restaurant_df) \
            .withColumn("geohash", geohash_udf(F.col("lat"), F.col("lng"))) \
            .withColumnsRenamed({'lat': 'restaurant_lat', 'lng': 'restaurant_lng'}) \
            .dropDuplicates(["restaurant_franchise_id", "geohash"])

        logger.info("Performed union of nulls and non-nulls and created geohash in restaurant data.")
        logger.info("First 10 rows in restaurants_df:")
        restaurants_df.show(10, truncate=False)
        logger.info("Schema of restaurants_df:")
        restaurants_df.printSchema()

        # reading weather data from partitioned parquet
        weather_df = spark.read.parquet("data/source/weather") \
            # .limit(1_000_000)

        # applying geohash adn dropping duplicates
        weather_df = weather_df \
            .withColumn("geohash", geohash_udf(F.col("lat"), F.col("lng"))) \
            .withColumnsRenamed({'lat': 'weather_lat', 'lng': 'weather_lng'}) \
            .dropDuplicates(["geohash", "year", "month", "day"])

        logger.info("Read data, created geohash, and removed duplicates in weather data.")
        logger.info(f"First 10 rows in weather_df: {weather_df.show(10, truncate=False)}")
        logger.info("Schema of weather_df:")
        weather_df.printSchema()

        # creating new dataframe joining restaurant to weather dataframe
        enriched_df = weather_df \
            .join(restaurants_df, on=["geohash"], how="left") \
            .filter(F.col("restaurant_lat").isNotNull()) \
            .dropDuplicates(["geohash", "year", "month", "day", "restaurant_franchise_id"])

        logger.info("Successfully joined restaurant and weather dataframes using geohash.")
        logger.info(f"First 10 rows in enriched_df: {enriched_df.show(10, truncate=False)}")
        logger.info("Schema of enriched_df:")
        enriched_df.printSchema()
        logger.info("Plan of query:")
        enriched_df.explain()

        # write saving previous partitioning
        enriched_df.repartition(3).write.partitionBy("year", "month", "day").mode("overwrite").parquet("data/enriched")
        logger.info("Successfully wrote repartitioned enriched dataframe.")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

    finally:
        spark.stop()
        logger.info("Successfully finished.")


if __name__ == '__main__':
    main()
