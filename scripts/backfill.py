"""
Historical Weather Data Backfill
Fetches past 30 days of weather data using Open-Meteo (free, no API key)
"""
import os
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
import psycopg2
from psycopg2.extras import Json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Config
LAT = 29.3956
LON = 71.6836
CITY = "Bahawalpur"
RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "/app/raw_data"))
RAW_DATA_DIR.mkdir(exist_ok=True)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "weather_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "weather_pass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather_db")


def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD, dbname=POSTGRES_DB
    )


def fetch_historical_day(date_str: str) -> dict:
    """Fetch hourly historical weather for a specific date."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": date_str,
        "end_date": date_str,
        "hourly": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,rain,snowfall,weather_code,cloud_cover,pressure_msl,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m",
        "timezone": "Asia/Karachi"
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch {date_str}: {e}")
        return None


def save_raw_to_db(conn, date_str: str, data: dict) -> str:
    """Save raw historical data to raw_weather table."""
    cursor = conn.cursor()

    # Parse the date for proper ingestion_date
    ingestion_date = datetime.strptime(date_str, "%Y-%m-%d").date()

    cursor.execute("""
        INSERT INTO raw_weather (fetched_at, source, api_endpoint, city, country,
                                latitude, longitude, raw_json, http_status, ingestion_date, processed)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (
        datetime.now(timezone.utc),
        "openmeteo",
        "archive",
        CITY,
        "PK",
        LAT, LON,
        Json(data),
        200,
        ingestion_date,
        True  # Mark as processed since we'll transform inline
    ))

    raw_id = cursor.fetchone()[0]
    conn.commit()
    return str(raw_id)


def transform_and_load_historical(conn, raw_id: str, data: dict, date_str: str):
    """Transform hourly historical data and load to staging."""
    cursor = conn.cursor()
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])

    records_loaded = 0
    ingestion_date = datetime.strptime(date_str, "%Y-%m-%d").date()

    for i, t in enumerate(times):
        try:
            fetched_at = datetime.fromisoformat(t.replace("Z", "+00:00"))
        except:
            continue

        temp = hourly.get("temperature_2m", [])[i] if i < len(hourly.get("temperature_2m", [])) else None
        humidity = hourly.get("relative_humidity_2m", [])[i] if i < len(hourly.get("relative_humidity_2m", [])) else None
        pressure = hourly.get("pressure_msl", [])[i] if i < len(hourly.get("pressure_msl", [])) else None

        # Skip if core fields are missing
        if temp is None or humidity is None:
            continue

        # Simple quality check
        if temp < -90 or temp > 60 or humidity < 0 or humidity > 100:
            continue

        cursor.execute("""
            INSERT INTO stg_weather_current (
                raw_id, fetched_at, source, city, country, latitude, longitude,
                temperature, feels_like, pressure_hpa, humidity_percent,
                wind_speed_ms, wind_deg, wind_gust_ms,
                weather_main, weather_description, clouds_percent,
                rain_1h_mm, snow_1h_mm, data_quality_score, ingestion_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            raw_id, fetched_at, "openmeteo", CITY, "PK", LAT, LON,
            temp,
            hourly.get("apparent_temperature", [])[i] if i < len(hourly.get("apparent_temperature", [])) else None,
            pressure,
            humidity,
            hourly.get("wind_speed_10m", [])[i] if i < len(hourly.get("wind_speed_10m", [])) else None,
            hourly.get("wind_direction_10m", [])[i] if i < len(hourly.get("wind_direction_10m", [])) else None,
            hourly.get("wind_gusts_10m", [])[i] if i < len(hourly.get("wind_gusts_10m", [])) else None,
            f"WMO {hourly.get('weather_code', [])[i]}" if i < len(hourly.get("weather_code", [])) else None,
            None,
            hourly.get("cloud_cover", [])[i] if i < len(hourly.get("cloud_cover", [])) else None,
            hourly.get("precipitation", [])[i] if i < len(hourly.get("precipitation", [])) else None,
            hourly.get("snowfall", [])[i] if i < len(hourly.get("snowfall", [])) else None,
            1.0,  # Historical data from archive is high quality
            ingestion_date
        ))
        records_loaded += 1

    conn.commit()
    return records_loaded


def run_backfill(days: int = 30):
    """Backfill historical data for the past N days."""
    logger.info(f"Starting backfill for past {days} days")

    conn = get_db_connection()
    total_loaded = 0

    try:
        for i in range(days, 0, -1):
            date_obj = datetime.now(timezone.utc) - timedelta(days=i)
            date_str = date_obj.strftime("%Y-%m-%d")

            logger.info(f"Fetching historical data for {date_str}")

            data = fetch_historical_day(date_str)
            if not data:
                continue

            # Save raw
            raw_id = save_raw_to_db(conn, date_str, data)

            # Transform and load
            loaded = transform_and_load_historical(conn, raw_id, data, date_str)
            total_loaded += loaded

            logger.info(f"  Loaded {loaded} hourly records for {date_str}")

        logger.info(f"Backfill complete. Total records loaded: {total_loaded}")

    finally:
        conn.close()


if __name__ == "__main__":
    run_backfill(30)
