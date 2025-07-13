import os
import functions_framework
import requests
import sqlalchemy
from datetime import datetime
from google.cloud.sql.connector import Connector
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

connector = Connector()

def get_db_connection():
    logger.debug("Attempting to connect to Cloud SQL")
    try:
        conn = connector.connect(
            os.environ.get("INSTANCE_CONNECTION_NAME"),
            "pymysql",
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASS"),
            db=os.environ.get("DB_NAME")
        )
        logger.debug("Successfully connected to Cloud SQL")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise

def create_table_if_not_exists():
    logger.debug("Checking/creating weather_data table")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS weather_data (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        city VARCHAR(100),
                        temperature FLOAT,
                        humidity INT,
                        timestamp DATETIME
                    )
                """)
                conn.commit()
                logger.debug("Table checked/created successfully")
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        raise

@functions_framework.http
def fetch_and_store_data(request):
    logger.debug("Function fetch_and_store_data triggered")
    try:
        logger.debug(f"Environment variables: INSTANCE_CONNECTION_NAME={os.environ.get('INSTANCE_CONNECTION_NAME')}, "
                     f"DB_USER={os.environ.get('DB_USER')}, DB_NAME={os.environ.get('DB_NAME')}")
        api_key = os.environ.get("OPENWEATHER_API_KEY")
        if not api_key:
            logger.error("OPENWEATHER_API_KEY is not set")
            raise ValueError("OPENWEATHER_API_KEY is not set")
        city = "London"
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
        logger.debug(f"Sending request to {url}")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logger.debug(f"Received response: {data}")
        temperature = data["main"]["temp"]
        humidity = data["main"]["humidity"]
        timestamp = datetime.now()
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO weather_data (city, temperature, humidity, timestamp) VALUES (%s, %s, %s, %s)",
                    (city, temperature, humidity, timestamp)
                )
                conn.commit()
                logger.debug("Data inserted into database")
        logger.info(f"Successfully stored data for {city}: {temperature}°C, {humidity}%")
        return {"status": "success", "message": "Data fetched and stored"}, 200
    except Exception as e:
        logger.error(f"Error in fetch_and_store_data: {str(e)}", exc_info=True)
        return {"status": "error", "message": str(e)}, 500
    finally:
        connector.close()
