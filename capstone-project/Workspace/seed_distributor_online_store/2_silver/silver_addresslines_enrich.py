# Databricks notebook source
!pip install geocoder python-Levenshtein

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath('/Workspace/Repos/zhastay_yeltay@epam.com/utils/'))

from delta.tables import *
from pyspark.sql import functions as F
import requests
import geocoder
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, LongType

from pyspark.sql.window import Window
from pyspark.sql import DataFrame

import pandas as pd


from init import *
init_spark()

from util_logger import init_logger
dbutils.widgets.text('task', "test_logger")
logger = init_logger(dbutils.widgets.get('task'))

# COMMAND ----------

def get_address_info(address):
    try:
        g = geocoder.bing(address, key='Aq_LUq5E6MHRgYGwGSj0GNir7jKS_vZgDfRBqmBjpBNxkN9P5tXbQYA_hruk1XdS')
        data = g.json
        return (
            data['lat'] if 'lat' in data else None,
            data['lng'] if 'lng' in data else None,
            data['postal'] if 'postal' in data else None,
            data['raw']['address']['adminDistrict2'] if 'adminDistrict2' in data['raw']['address'] else None,
            data['neighborhood'] if 'neighborhood' in data else None,
            data['address'] if 'address' in data else None
        )
    except Exception as e:
        print(f'address: {address}, {str(e)}')
        return (None, None, None, None, None, None)
    
coord_schema = StructType([
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("postal_code", StringType(), True),
    StructField("county", StringType(), True),
    StructField("neighborhood", StringType(), True),
    StructField("completed_address", StringType(), True),
])

get_address_info_udf = F.udf(get_address_info, coord_schema)

print(get_address_info('1 Boren Ave, Port Townsend, Washington, US'))
print(get_address_info('4932 20, Port Townsend, Washington'))
print(get_address_info('7266 20, Port Townsend, Washington'))
print(get_address_info('313782 Us 101 street, Brinnon city, Washington state, US country'))
print(get_address_info('304753 Us 101 street, Brinnon city, Washington state, US country'))



# COMMAND ----------

street_mapping = {
    'N': 'North',
    'S': 'South',
    'W': 'West',
    'E': 'East',
    'NW': 'Northwest',
    'NE': 'Northeast',
    'SW': 'Southwest',
    'SE': 'Southeast',
    'Ave': 'Avenue',
    'St': 'Street',
    'Rd': 'Road',
    'Dr': 'Drive',
    'Pl': 'Place',
    'Ln': 'Lane',
    'Way': 'Way',  # 'Way' does not need abbreviation
    'Blvd': 'Boulevard',
    'Ct': 'Court',
    'Hwy': 'Highway',
    'Pkwy': 'Parkway'
}

def expand_street_names(address):
    result = []
    address = address.replace(' street', '')
    for word in address.split():
        if word[-1] == ',':
            cleaned_word = word.replace(',', '').strip()
            mapped = street_mapping.get(cleaned_word, cleaned_word)+','
        else:
            mapped = street_mapping.get(word, word)
        result.append(mapped)
    return ' '.join(result)

expand_street_names_udf = udf(expand_street_names, StringType())



street_mapping_reverse = {value: key for key, value in street_mapping.items()}

def abbreviate_street_names(address):
    result = []
    for word in address.split():
        if word[-1] == ',':
            cleaned_word = word.replace(',', '').strip()
            mapped = street_mapping_reverse.get(cleaned_word, cleaned_word)+','
        else:
            mapped = street_mapping_reverse.get(word, word)
        result.append(mapped)
    return ' '.join(result)




print(expand_street_names('10319 NE 114th Pl, Kirkland, WA 98033'))
print(abbreviate_street_names('10319 Northeast 114th Place, Kirkland, Washington 98033'))

# COMMAND ----------

try:
    logger.info(f"Loading addressline_enriched Delta table from zhastay_yeltay_02_silver.addressline_enriched.")
    enriched_addresses_old = spark.table(
        "hive_metastore.zhastay_yeltay_02_silver.addressline_enriched"
    )

    # Log processing start
    logger.info("Starting to process addressline entries.")

    max_id = enriched_addresses_old.agg(F.max('id')).head()[0]
    if not max_id:
        max_id = 0
    print(max_id)

    unique_addresslines = (
        spark.table("hive_metastore.zhastay_yeltay_02_silver.addresses")
        .select("addressline", "country", "state", "city")
        .distinct()
        .withColumn(
            'full_address',
            F.concat(
                F.col("addressline"),
                F.lit(", "),
                # F.lit(" street, "), # Adding that is a street clarification to API
                F.col("city"),
                F.lit(", "),
                # F.lit(" city, "), # Adding that is a city clarification to API
                F.col("state"),
                F.lit(", "),
                # F.lit(" state, "), # Adding that is a state clarification to API
                F.col("country")
            )
        )
        .alias("new")
        .join(
            enriched_addresses_old.alias("old"),
            (F.col("new.addressline") == F.col("old.addressline"))
            & (F.col("new.city") == F.col("old.city"))
            & (F.col("new.state") == F.col("old.state"))
            & (F.col("new.country") == F.col("old.country")),
            how="leftanti",
        )
    )


    pandas_df = unique_addresslines.toPandas()

    # Log the outcome
    logger.info(f"Processed unique addresslines.")
except Exception as e:
    logger.error("An error occurred while processing addressline entries.", exc_info=True)
    raise
    

# COMMAND ----------

import time


processed_list = []
num_retry = 2


logger.info("Starting to get data from Bing Maps API.")

for it_num, value in enumerate(pandas_df.to_dict('records')):
    query = (
        value['full_address']
        .replace('Us 101', 'US Highway 101')
    )
    retry = 0
    lat, lng, postal_code, county, neighborhood, completed_address = get_address_info(query)

    distance = Levenshtein.distance(abbreviate_street_names(query.split(', ')[0]), abbreviate_street_names(str(completed_address).split(', ')[0]))
    while ((not completed_address) or distance > 10) and retry < num_retry:
        address_line, city, state, country = query.split(', ')
        query = f"{address_line}, {city} city, {state} state, United States"
        time.sleep(0.5)
        print(f"\n i: {it_num}, retry: {retry}, distance: {distance}")
        lat, lng, postal_code, county, neighborhood, completed_address = get_address_info(query)
        
        distance = Levenshtein.distance(abbreviate_street_names(query.split(', ')[0]), abbreviate_street_names(completed_address.split(', ')[0]))
        retry += 1
    

    value['lat'] = lat
    value['lng'] = lng
    value['postal_code'] = postal_code
    value['county'] = county
    value['neighborhood'] = neighborhood
    value['completed_address'] = completed_address
    processed_list.append(value)
    print(f"{it_num} / {pandas_df.shape[0]-1}, is None: {value['completed_address'] == None}, retry: {retry}, distance: {distance} \r", end='', flush=True)
    time.sleep(0.1)


logger.info("Finished of getting data from Bing Maps API.")

# COMMAND ----------

print(len([i for i in processed_list if i['postal_code'] == None]))

# COMMAND ----------

try:
    logger.info("Starting to process API entries.")

    if len(processed_list) > 0:
        
        processed_df = (
            spark.createDataFrame(processed_list)
            .withColumn("old_house_number", F.split(F.col("full_address"), " ")[0])
            .withColumn("new_house_number", F.split(F.col("completed_address"), " ")[0])
            .withColumn(
                "levenshtein_distance",
                F.levenshtein(
                    expand_street_names_udf(F.split(F.col("full_address"), ", ")[0]),
                    expand_street_names_udf(F.split(F.col("completed_address"), ", ")[0]),
                ),
            )
            .filter(
                F.col("completed_address").isNotNull()
                # & F.col("postal_code").isNotNull()
                & F.col("lat").isNotNull()
                & F.col("lng").isNotNull()
                # & (F.col('levenshtein_distance') <= 5)
                # & (F.col('old_house_number') == F.col('new_house_number'))
            )
            .withColumn("id", max_id + F.row_number().over(Window.orderBy("full_address")))
            .select(
                "id",
                "addressline",
                "country",
                "state",
                "city",
                "full_address",
                "completed_address",
                "lat",
                "lng",
                "postal_code",
                "county",
                "neighborhood",
            )
        )

        processed_df.write.format("delta").mode("append").saveAsTable(
            f"{catalog_name}.{schema_silver_name}.addressline_enriched"
        )

        # Log the outcome
        logger.info(f"Processed enriched AddressLines from Bing API.")
    else:
        logger.info(f"There is no new AddressLines.")

except Exception as e:
    logger.error("An error occurred while processing enriched AddressLines entries.", exc_info=True)
    raise


# COMMAND ----------

addressline_enriched_count = spark.table("hive_metastore.zhastay_yeltay_02_silver.addressline_enriched").count()
addresses_count = spark.table("hive_metastore.zhastay_yeltay_02_silver.addresses").select('addressline').distinct().count()

logger.info(f'All data processed: {addressline_enriched_count == addresses_count}')

# COMMAND ----------

