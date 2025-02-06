# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
import calendar
from itemloaders.processors import MapCompose
from datetime import datetime
from decimal import Decimal

def text_to_num(text):
    d = {'K': 3}
    if text[-1] in d:
        num, magnitude = text[:-1], text[-1]
        return int(Decimal(num) * 10 ** d[magnitude])
    else:
        return int(Decimal(text))

def getNumericResponse(text):
    if text == None:
        responses = 0
    else:
        responses = text.split()[0]
    return responses

def getNumericReadTime(text):
    if text != None:
        return text.split()[0]

def parse_date(date_str):
    """Try to parse the date string with a given year."""
    try:
        return datetime.strptime(date_str, "%b %d, %Y")
    except ValueError:
        return None

def is_valid_date(year, month, day):
    """Check if a date is valid for the given year."""
    try:
        datetime(year, month, day)
        return True
    except ValueError:
        return False

def getPublishedDate(published_date):
    # Get current year
    current_year = datetime.now().year

    # Try parsing with the year first
    date_object = parse_date(published_date)
    
    if date_object is None:
        # Try parsing without the year
        try:
            date_object = parse_date(f"{published_date}, {current_year}")
            if date_object is None:
                raise ValueError(f"Date format for '{published_date}' is not recognized or invalid.")
            date_object = date_object.replace(year=current_year)
        except ValueError:
            raise ValueError(f"Date format for '{published_date}' is not recognized")

    # Extract year, month, and day
    year = date_object.year
    month = date_object.month
    day = date_object.day

    # Ensure the date is valid considering leap years
    if not is_valid_date(year, month, day):
        raise ValueError(f"Invalid date detected: {published_date}")

    return date_object


class ArticlesCollectorItem(scrapy.Item):
    author = scrapy.Field()
    title = scrapy.Field()
    subtitle_preview = scrapy.Field()
    collection = scrapy.Field()
    read_time = scrapy.Field(input_processor=MapCompose(getNumericReadTime))
    claps = scrapy.Field(input_processor=MapCompose(text_to_num))
    responses = scrapy.Field(input_processor=MapCompose(getNumericResponse))
    published_date = scrapy.Field(input_processor=MapCompose(getPublishedDate))
    scraped_date = scrapy.Field()
