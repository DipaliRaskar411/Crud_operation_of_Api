import requests
import mysql.connector
from mysql.connector import Error


session = requests.Session()
session.trust_env = False  

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Root@123",
    "database": "demo"
}

API_URL = "http://127.0.0.1:5000/data"

def fetch_data_from_api(url):
    """Fetch data from API endpoint."""
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        print(" API connection successful!")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data from API: {e}")
        return None

def insert_data_to_mysql(data):
    """Insert fetched data into MySQL 'weather' table."""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS weather (
            id INT PRIMARY KEY,
            time DATETIME,
            temperature_2m FLOAT,
            relative_humidity_2m FLOAT,
            wind_speed_10m FLOAT,
            data_type VARCHAR(50),
            relative_humidity_6m FLOAT,
            temperature_4m FLOAT,
            wind_speed_15m FLOAT,
            temperature_5m FLOAT,
            relative_humidity_4m FLOAT,
            wind_speed_12m FLOAT,
            temperature_7m FLOAT
        )
        """
        cursor.execute(create_table_query)

        insert_query = """
        INSERT INTO weather (
            id, time, temperature_2m, relative_humidity_2m, wind_speed_10m, data_type,
            relative_humidity_6m, temperature_4m, wind_speed_15m, temperature_5m,
            relative_humidity_4m, wind_speed_12m, temperature_7m
        ) VALUES (
            %(id)s, %(time)s, %(temperature_2m)s, %(relative_humidity_2m)s, %(wind_speed_10m)s, %(data_type)s,
            %(relative_humidity_6m)s, %(temperature_4m)s, %(wind_speed_15m)s, %(temperature_5m)s,
            %(relative_humidity_4m)s, %(wind_speed_12m)s, %(temperature_7m)s
        )
        ON DUPLICATE KEY UPDATE
            time = VALUES(time),
            temperature_2m = VALUES(temperature_2m),
            relative_humidity_2m = VALUES(relative_humidity_2m),
            wind_speed_10m = VALUES(wind_speed_10m),
            data_type = VALUES(data_type),
            relative_humidity_6m = VALUES(relative_humidity_6m),
            temperature_4m = VALUES(temperature_4m),
            wind_speed_15m = VALUES(wind_speed_15m),
            temperature_5m = VALUES(temperature_5m),
            relative_humidity_4m = VALUES(relative_humidity_4m),
            wind_speed_12m = VALUES(wind_speed_12m),
            temperature_7m = VALUES(temperature_7m)
        """

      
        if isinstance(data, list):
            cursor.executemany(insert_query, data)
        else:
            cursor.execute(insert_query, data)

        connection.commit()
        print(f" Data inserted/updated successfully into 'weather' table.")

    except Error as e:
        print(f" MySQL Error: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    data = fetch_data_from_api(API_URL)
    if data:
        insert_data_to_mysql(data) 