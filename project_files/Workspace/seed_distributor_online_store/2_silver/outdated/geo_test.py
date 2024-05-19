# Databricks notebook source
'85 Woodview Road, Henagar, Alabama, US'

# COMMAND ----------

!pip install geocoder

# COMMAND ----------

import requests
import geocoder
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, LongType


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
print(get_address_info('85 Woodview Road street, Henagar city, Alabama state, US country'))



# COMMAND ----------

