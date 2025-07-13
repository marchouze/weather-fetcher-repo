import os
import functions_framework
import requests
import sqlalchemy
from datetime import datetime
from google.cloud.sql.connector import Connector
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Cloud SQL Connector
connector = Connector()

def get_db_connection():
    try:
        conn = connector.connect(
            os.environ.get("INSTANCE_CONNECTION_NAME"),
            "pymysql",
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASS"),
            db=os.environ.get("DB_NAME")
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise

def create_table_if_not_exists():
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
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        raise

@functions_framework.http
def fetch_and_store_data(request):
    try:
        # Fetch public data (using OpenWeatherMap as an example)
        api_key = os.environ.get("OPENWEATHER_API_KEY")
        city = "London"  # Example city
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
        
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Extract relevant data
        temperature = data["main"]["temp"]
        humidity = data["main"]["humidity"]
        timestamp = datetime.now()

        # Store in Cloud SQL
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO weather_data (city, temperature, humidity, timestamp) VALUES (%s, %s, %s, %s)",
                    (city, temperature, humidity, timestamp)
                )
                conn.commit()

        logger.info(f"Successfully stored data for {city}: {temperature}°C, {humidity}%")
        return {"status": "success", "message": "Data fetched and stored"}, 200

    except Exception as e:
        logger.error(f"Error in fetch_and_store_data: {str(e)}")
        return {"status": "error", "message": str(e)}, 500

    finally:
        connector.close()
