from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

from opencage.geocoder import OpenCageGeocode
import pygeohash
import logging

# this is key saver... Create this file after cloning repos and define new variable OpenCageGeocodeApiKey with key
import constants

geocoder = OpenCageGeocode(constants.OpenCageGeocodeApiKey)


def get_coordinate(franchise_name: str, city: str, country: str) -> tuple:
    """
    Get geographical coordinates (latitude, longitude) for a given franchise name, city, and country.

    Args:
        franchise_name (str): The name of the franchise.
        city (str): The city associated with the franchise.
        country (str): The country code of the location.

    Returns:
        tuple: A tuple containing latitude and longitude. Returns (None, None) if geocoding fails.

    Note:
        This function relies on an external geocoding service.

    Raises:
        Exception: An exception is caught if geocoding fails for any reason.

    """
    query = f'{franchise_name}, {city}'
    try:
        coordinate = geocoder.geocode(query, countrycode=country.lower(), limit=1)
        if coordinate is not None:
            return coordinate[0]['geometry']['lat'], coordinate[0]['geometry']['lng']
        else:
            logging.warning(f"Geocoding no possible for {query}")
    except Exception as e:
        logging.warning(f"Geocoding failed for {query}. Error: {str(e)}")
    return None, None


def get_geohash(lat: float, lng: float) -> str:
    """
    Generate a geohash string based on the given latitude and longitude.

    Args:
        lat (float): Latitude coordinate.
        lng (float): Longitude coordinate.

    Returns:
        str: A geohash string representing the location.

    Note:
        This function uses the pygeohash library for geohashing.

    Warning:
        If either latitude or longitude is None, the function returns None and logs a warning.

    """
    if lat is None or lng is None:
        logging.warning(f"Geohash generation not possible. Latitude or longitude is missing.")
        return None
    if not ((-90.0 <= lat) and (lat <= 90.0)) or not ((-180.0 <= lng) and (lng <= 180.0)):
        logging.warning(f"Geohash generation not possible. Latitude or longitude is out of bound.")
        return None
    return pygeohash.encode(lat, lng, precision=4)


# schema with nullable latitude and longitude
coord_schema = StructType([
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True)
])

get_coordinate_udf = F.udf(get_coordinate, coord_schema)
geohash_udf = F.udf(get_geohash, StringType())

if __name__ == "__main__":
    # mini test
    print(get_coordinate("Savoria", "Dillon", "US"))
    print(get_geohash(181, 180))
