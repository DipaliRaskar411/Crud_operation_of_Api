import requests
import mysql.connector
import pandas as pd


latitude = float(input("Enter latitude: "))
longitude = float(input("Enter longitude: "))
current_params = input("Enter current parameters (e.g. temperature_2m,wind_speed_10m): ")
hourly_params = input("Enter hourly parameters (e.g. temperature_2m,relative_humidity_2m,wind_speed_10m): ")


current_list = [p.strip() for p in current_params.split(",") if p.strip()]
hourly_list = [p.strip() for p in hourly_params.split(",") if p.strip()]


API_URL = "http://127.0.0.1:5000"
PARAMS = {
    "latitude": latitude,
    "longitude": longitude,
    "current": ",".join(current_list),
    "hourly": ",".join(hourly_list)
}


DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Root@123",
    "database": "demo"
}


response = requests.get(API_URL, params=PARAMS)
if response.status_code != 200:
    print(" API request failed:", response.text)
    exit()

data = response.json()
if data.get("error"):
    print(" API Error:", data.get("reason", "Unknown error"))
    exit()


conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()


def ensure_columns_exist(column_names):
    """Check if columns exist in weather_info; add missing ones."""
    cursor.execute("SHOW COLUMNS FROM weather_info")
    existing_columns = [row[0] for row in cursor.fetchall()]

    for col in column_names:
        if col not in existing_columns:
            alter_query = f"ALTER TABLE weather_info ADD COLUMN `{col}` FLOAT NULL"
            cursor.execute(alter_query)
            print(f" Added missing column: {col}")
    conn.commit()


all_params = list(set(current_list + hourly_list))
ensure_columns_exist(all_params)


current = data.get("current", {})
if current:
    time_val = current.get("time")

    
    values = [current.get(param) for param in current_list]

    columns = ", ".join([f"`{c}`" for c in current_list])
    placeholders = ", ".join(["%s"] * len(current_list))

    query = f"""
        INSERT INTO weather_info (time, {columns}, data_type)
        VALUES (%s, {placeholders}, %s)
    """
    cursor.execute(query, [time_val, *values, "current"])
    conn.commit()
    print(" Current weather inserted successfully.")


hourly = data.get("hourly", {})
if hourly:
    valid_keys = {k: v for k, v in hourly.items() if isinstance(v, list)}
    if not valid_keys:
        print(" No valid hourly data found.")
    else:
        df = pd.DataFrame(valid_keys)
        if "time" not in df.columns:
            print(" No time column found in hourly data.")
        else:
            for _, row in df.iterrows():
                values = [row.get(param) for param in hourly_list]
                columns = ", ".join([f"`{c}`" for c in hourly_list])
                placeholders = ", ".join(["%s"] * len(hourly_list))

                query = f"""
                    INSERT INTO weather_info (time, {columns}, data_type)
                    VALUES (%s, {placeholders}, %s)
                """
                cursor.execute(query, [row["time"], *values, "hourly"])

            conn.commit()
            print(f"Inserted {len(df)} hourly records successfully.")


cursor.close()
conn.close()
print("All data saved successfully into the weather_info table!")
