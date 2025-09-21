import os

import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")

weather_url = (
    f"http://api.weatherstack.com/current?access_key={WEATHER_API_KEY}&query=New York"
)


def fetch_data() -> dict:
    """Fetch data from a REST API endpoint.

    Args:
        url (str): The API endpoint URL.

    Returns:
        dict: The JSON response from the API or an error message.
    """
    logger.info("Fetching data")
    try:
        response = requests.get(weather_url, timeout=10)
        response.raise_for_status()
        logger.info("Data fetched successfully")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        raise


def mock_fetch_data() -> dict:
    """Mock function to simulate fetching data from a REST API endpoint.

    Args:
        url (str): The API endpoint URL.

    Returns:
        dict: A mock JSON response.
    """
    logger.info("Mock fetching data")
    mock_response = {
        "request": {
            "type": "City",
            "query": "New York, United States of America",
            "language": "en",
            "unit": "m",
        },
        "location": {
            "name": "New York",
            "country": "United States of America",
            "region": "New York",
            "lat": "40.714",
            "lon": "-74.006",
            "timezone_id": "America/New_York",
            "localtime": "2025-09-20 04:36",
            "localtime_epoch": 1758342960,
            "utc_offset": "-4.0",
        },
        "current": {
            "observation_time": "08:36 AM",
            "temperature": 18,
            "weather_code": 116,
            "weather_icons": [
                "https://cdn.worldweatheronline.com/images/wsymbols01_png_64/wsymbol_0004_black_low_cloud.png"
            ],
            "weather_descriptions": ["Partly Cloudy "],
            "astro": {
                "sunrise": "06:42 AM",
                "sunset": "06:56 PM",
                "moonrise": "05:22 AM",
                "moonset": "06:27 PM",
                "moon_phase": "Waning Crescent",
                "moon_illumination": 3,
            },
            "air_quality": {
                "co": "431.05",
                "no2": "38.665",
                "o3": "38",
                "so2": "9.62",
                "pm2_5": "11.1",
                "pm10": "11.285",
                "us-epa-index": "1",
                "gb-defra-index": "1",
            },
            "wind_speed": 18,
            "wind_degree": 34,
            "wind_dir": "NNE",
            "pressure": 1022,
            "precip": 0,
            "humidity": 45,
            "cloudcover": 0,
            "feelslike": 18,
            "uv_index": 0,
            "visibility": 16,
            "is_day": "no",
        },
    }
    return mock_response
