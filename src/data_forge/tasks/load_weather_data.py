import os
from datetime import datetime
from loguru import logger

from data_forge.connections.apis.weather import mock_fetch_data, fetch_data
from data_forge.connections.postgres import connect_to_postgres, create_table, insert_data
from data_forge.models.schemas import load_yaml

def load_weather_data(layer: str = "source", table: str = "raw_weather_data"):
    """
    Load weather data into the specified layer and table.

    Args:
        layer (str, optional): The data layer to load into. Defaults to "source".
        table (str, optional): The table to load data into. Defaults to "raw_weather_data".
    """
    conn = None
    try:
        logger.info(f"Loading weather data for {layer}.{table}")
        
        # Get database configuration from environment variables
        db_host = os.getenv("POSTGRES_HOST", "db")
        db_name = os.getenv("POSTGRES_DB", "db")
        db_user = os.getenv("POSTGRES_USER", "user")
        db_password = os.getenv("POSTGRES_PASSWORD", "password")
        db_port = int(os.getenv("POSTGRES_PORT", "5432"))
        
        # Fetch data
        # data = mock_fetch_data()
        data = fetch_data()
        
        # Prepare data for insertion - flattened structure
        flattened_data = {
            # Request fields
            "request_type": data["request"]["type"],
            "request_query": data["request"]["query"],
            "request_language": data["request"]["language"],
            "request_unit": data["request"]["unit"],
            
            # Location fields
            "location_name": data["location"]["name"],
            "location_country": data["location"]["country"],
            "location_region": data["location"]["region"],
            "location_lat": float(data["location"]["lat"]),
            "location_lon": float(data["location"]["lon"]),
            "location_timezone_id": data["location"]["timezone_id"],
            "location_localtime": data["location"]["localtime"],
            "location_localtime_epoch": data["location"]["localtime_epoch"],
            "location_utc_offset": float(data["location"]["utc_offset"]),
            
            # Current weather fields
            "current_observation_time": data["current"]["observation_time"],
            "current_temperature": data["current"]["temperature"],
            "current_weather_code": data["current"]["weather_code"],
            "current_weather_icons": data["current"]["weather_icons"][0] if data["current"]["weather_icons"] else None,
            "current_weather_descriptions": data["current"]["weather_descriptions"][0] if data["current"]["weather_descriptions"] else None,
            "current_wind_speed": data["current"]["wind_speed"],
            "current_wind_degree": data["current"]["wind_degree"],
            "current_wind_dir": data["current"]["wind_dir"],
            "current_pressure": data["current"]["pressure"],
            "current_precip": data["current"]["precip"],
            "current_humidity": data["current"]["humidity"],
            "current_cloudcover": data["current"]["cloudcover"],
            "current_feelslike": data["current"]["feelslike"],
            "current_uv_index": data["current"]["uv_index"],
            "current_visibility": data["current"]["visibility"],
            "current_is_day": data["current"]["is_day"],
            
            # Astro fields
            "astro_sunrise": data["current"]["astro"]["sunrise"],
            "astro_sunset": data["current"]["astro"]["sunset"],
            "astro_moonrise": data["current"]["astro"]["moonrise"],
            "astro_moonset": data["current"]["astro"]["moonset"],
            "astro_moon_phase": data["current"]["astro"]["moon_phase"],
            "astro_moon_illumination": data["current"]["astro"]["moon_illumination"],
            
            # Air quality fields
            "air_quality_co": float(data["current"]["air_quality"]["co"]),
            "air_quality_no2": float(data["current"]["air_quality"]["no2"]),
            "air_quality_o3": float(data["current"]["air_quality"]["o3"]),
            "air_quality_so2": float(data["current"]["air_quality"]["so2"]),
            "air_quality_pm2_5": float(data["current"]["air_quality"]["pm2_5"]),
            "air_quality_pm10": float(data["current"]["air_quality"]["pm10"]),
            "air_quality_us_epa_index": data["current"]["air_quality"]["us-epa-index"],
            "air_quality_gb_defra_index": data["current"]["air_quality"]["gb-defra-index"],
            
            # Metadata fields - THESE WERE MISSING!
            "inserted_at": datetime.utcnow(),
            "data_source": "mock_api"
        }

        # Connect to database
        conn = connect_to_postgres(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port,
        )
        
        # Load schema
        schema = load_yaml(f"/opt/airflow/config/schemas/{layer}/{table}.yaml")
        
        # Create table and insert data
        create_table(connection=conn, layer=layer, table=table, schema=schema)
        insert_data(connection=conn, layer=layer, table=table, data=flattened_data, schema=schema)
        
        logger.info("Data inserted successfully.")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
                logger.info("Database connection closed.")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")