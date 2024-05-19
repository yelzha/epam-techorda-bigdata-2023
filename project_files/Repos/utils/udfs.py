from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BooleanType
import logging
import re


def clean_text(raw_text) -> str:
    try:
        # Remove special characters
        text = re.sub(r"[^a-zA-Z0-9'\s]", ' ', raw_text)
        
        text = ' '.join(s for s in text.split())

        return text
    except Exception as e:
        logging.warning(f"Text cleansing failed for {raw_text}. Error: {str(e)}")
    return None

def clean_text_capitalize_words(raw_text) -> str:
    try:
        # Remove special characters
        text = re.sub(r"[^a-zA-Z0-9'\s]", ' ', raw_text)
        
        # Remove extra spaces and Capitalize the first letter of each sentence
        text = ' '.join(s.capitalize() for s in text.split())
        
        return text
    except Exception as e:
        logging.warning(f"Text cleansing failed for {raw_text}. Error: {str(e)}")
    return None



# patterns
address_pattern = r"^\d+b?\s[A-Z0-9][a-z0-9']*(?:\s[A-Z0-9][a-z0-9']*)*$"
city_state_pattern = r"^[A-Z][a-z]+(?:\s[A-Z][a-z]+)*$"


def is_valid_address(address):
    return bool(re.match(address_pattern, address))

def is_valid_city_state(city):
    return bool(re.match(city_state_pattern, city))



clean_text_udf = F.udf(clean_text, StringType())
clean_text_capitalize_words_udf = F.udf(clean_text_capitalize_words, StringType())
capitalize_udf  = F.udf(lambda x: str(x).capitalize(), StringType())

is_valid_udf = F.udf(is_valid_address, BooleanType())
is_valid_city_state_udf = F.udf(is_valid_city_state, BooleanType())

