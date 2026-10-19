"""
Advanced Weather Data ETL Pipeline for Bahawalpur
Multi-source extraction, data quality checks, partitioning, and lineage tracking
"""
import os
import json
import logging
import time
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict

import requests
import psycopg2
from psycopg2.extras import execute_values, Json
import redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

@dataclass
class Config:
    OPENWEATHER_API_KEY: str = os.getenv("OPENWEATHER_API_KEY", "")
    OPENMETEO_ENABLED: bool = os.getenv("OPENMETEO_ENABLED", "true").lower() == "true"

    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "postgres")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "weather_user")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "weather_pass")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "weather_db")

    REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

    CITY: str = "Bahawalpur"
    LAT: float = 29.3956
    LON: float = 71.6836
    COUNTRY: str = "PK"

    RAW_DATA_DIR: Path = Path(os.getenv("RAW_DATA_DIR", "/app/raw_data"))

    # Rate limiting
    REQUEST_TIMEOUT: int = 30
    RETRY_ATTEMPTS: int = 3
    RETRY_DELAY: int = 2

CONFIG = Config()
CONFIG.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# DATABASE CONNECTIONS
# ============================================================

def get_db_connection():
    """Create PostgreSQL connection with retry logic."""
    for attempt in range(CONFIG.RETRY_ATTEMPTS):
        try:
            conn = psycopg2.connect(
                host=CONFIG.POSTGRES_HOST,
                port=CONFIG.POSTGRES_PORT,
                user=CONFIG.POSTGRES_USER,
                password=CONFIG.POSTGRES_PASSWORD,
                dbname=CONFIG.POSTGRES_DB,
                connect_timeout=10
            )
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"DB connection attempt {attempt + 1} failed: {e}")
            if attempt < CONFIG.RETRY_ATTEMPTS - 1:
                time.sleep(CONFIG.RETRY_DELAY * (attempt + 1))
    raise Exception("Failed to connect to database after all retries")


def get_redis_client():
    """Create Redis client for caching and deduplication."""
    try:
        client = redis.Redis(
            host=CONFIG.REDIS_HOST,
            port=CONFIG.REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=5
        )
        client.ping()
        return client
    except redis.ConnectionError:
        logger.warning("Redis not available, proceeding without caching")
        return None

# ============================================================
# DATA QUALITY
# ============================================================

class DataQualityChecker:
    """Validates data before loading to staging."""

    @staticmethod
    def check_temperature(temp: Optional[float]) -> tuple[bool, Optional[str]]:
        if temp is None:
            return False, "Temperature is null"
        if temp < -90 or temp > 60:
            return False, f"Temperature {temp}°C out of realistic range"
        return True, None

    @staticmethod
    def check_humidity(humidity: Optional[int]) -> tuple[bool, Optional[str]]:
        if humidity is None:
            return False, "Humidity is null"
        if humidity < 0 or humidity > 100:
            return False, f"Humidity {humidity}% out of range"
        return True, None

    @staticmethod
    def check_pressure(pressure: Optional[int]) -> tuple[bool, Optional[str]]:
        if pressure is None:
            return False, "Pressure is null"
        if pressure < 870 or pressure > 1085:
            return False, f"Pressure {pressure} hPa out of range"
        return True, None

    @staticmethod
    def calculate_quality_score(record: Dict) -> float:
        """Calculate data quality score (0.0 to 1.0)."""
        checks = [
            DataQualityChecker.check_temperature(record.get("temperature")),
            DataQualityChecker.check_humidity(record.get("humidity")),
            DataQualityChecker.check_pressure(record.get("pressure")),
        ]
        passed = sum(1 for ok, _ in checks if ok)
        return round(passed / len(checks), 2)

    @staticmethod
    def log_quality_issue(conn, table_name: str, issue_type: str, severity: str, 
                          description: str, affected_rows: int = 1, sample_data: Optional[Dict] = None):
        """Log data quality issue to database."""
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO data_quality_issues (table_name, issue_type, severity, description, affected_rows, sample_data)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (table_name, issue_type, severity, description, affected_rows, 
                  Json(sample_data) if sample_data else None))
            conn.commit()
        except Exception as e:
            logger.error(f"Failed to log quality issue: {e}")

# ============================================================
# PIPELINE TRACKING
# ============================================================

class PipelineTracker:
    """Tracks pipeline runs and lineage."""

    def __init__(self, conn):
        self.conn = conn
        self.run_id = None
        self.records_extracted = 0
        self.records_transformed = 0
        self.records_loaded = 0

    def start_run(self, pipeline_name: str, run_type: str = "full"):
        """Start tracking a pipeline run."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO pipeline_runs (pipeline_name, run_type, status)
            VALUES (%s, %s, 'running')
            RETURNING id
        """, (pipeline_name, run_type))
        self.run_id = cursor.fetchone()[0]
        self.conn.commit()
        logger.info(f"Pipeline run started: {self.run_id}")
        return self.run_id

    def update_status(self, status: str, error_message: Optional[str] = None):
        """Update pipeline run status."""
        if not self.run_id:
            return
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE pipeline_runs 
            SET status = %s, 
                completed_at = NOW(),
                records_extracted = %s,
                records_transformed = %s,
                records_loaded = %s,
                error_message = %s,
                duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))::int
            WHERE id = %s
        """, (status, self.records_extracted, self.records_transformed, 
              self.records_loaded, error_message, self.run_id))
        self.conn.commit()
        logger.info(f"Pipeline run {self.run_id} status: {status}")

# ============================================================
# EXTRACTION
# ============================================================

class WeatherExtractor:
    """Extracts weather data from multiple sources."""

    def __init__(self, redis_client=None):
        self.redis = redis_client
        self.session = requests.Session()

    def _make_request(self, url: str, params: Dict, source: str) -> tuple[Optional[Dict], int, float]:
        """Make HTTP request with retry logic and timing."""
        start_time = time.time()

        for attempt in range(CONFIG.RETRY_ATTEMPTS):
            try:
                response = self.session.get(url, params=params, timeout=CONFIG.REQUEST_TIMEOUT)
                response_time = (time.time() - start_time) * 1000

                if response.status_code == 200:
                    return response.json(), response.status_code, response_time
                elif response.status_code == 429:
                    logger.warning(f"Rate limited by {source}, waiting...")
                    time.sleep(60)
                else:
                    logger.error(f"{source} returned {response.status_code}: {response.text[:200]}")
                    return None, response.status_code, response_time

            except requests.exceptions.RequestException as e:
                logger.warning(f"{source} request attempt {attempt + 1} failed: {e}")
                if attempt < CONFIG.RETRY_ATTEMPTS - 1:
                    time.sleep(CONFIG.RETRY_DELAY * (attempt + 1))

        return None, 0, (time.time() - start_time) * 1000

    def extract_openweather_current(self) -> Optional[Dict]:
        """Extract current weather from OpenWeatherMap."""
        if not CONFIG.OPENWEATHER_API_KEY or CONFIG.OPENWEATHER_API_KEY == "your_api_key_here":
            logger.error("OPENWEATHER_API_KEY not set!")
            return None

        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            "lat": CONFIG.LAT,
            "lon": CONFIG.LON,
            "appid": CONFIG.OPENWEATHER_API_KEY,
            "units": "metric"
        }

        data, status, response_time = self._make_request(url, params, "openweather_current")

        if data:
            return {
                "source": "openweather",
                "api_endpoint": "current",
                "data": data,
                "http_status": status,
                "response_time_ms": int(response_time)
            }
        return None

    def extract_openweather_forecast(self) -> Optional[Dict]:
        """Extract 5-day forecast from OpenWeatherMap."""
        if not CONFIG.OPENWEATHER_API_KEY or CONFIG.OPENWEATHER_API_KEY == "your_api_key_here":
            return None

        url = "https://api.openweathermap.org/data/2.5/forecast"
        params = {
            "lat": CONFIG.LAT,
            "lon": CONFIG.LON,
            "appid": CONFIG.OPENWEATHER_API_KEY,
            "units": "metric"
        }

        data, status, response_time = self._make_request(url, params, "openweather_forecast")

        if data:
            return {
                "source": "openweather",
                "api_endpoint": "forecast",
                "data": data,
                "http_status": status,
                "response_time_ms": int(response_time)
            }
        return None

    def extract_openweather_air_quality(self) -> Optional[Dict]:
        """Extract air quality data from OpenWeatherMap."""
        if not CONFIG.OPENWEATHER_API_KEY or CONFIG.OPENWEATHER_API_KEY == "your_api_key_here":
            return None

        url = "https://api.openweathermap.org/data/2.5/air_pollution"
        params = {
            "lat": CONFIG.LAT,
            "lon": CONFIG.LON,
            "appid": CONFIG.OPENWEATHER_API_KEY
        }

        data, status, response_time = self._make_request(url, params, "openweather_air_quality")

        if data:
            return {
                "source": "openweather",
                "api_endpoint": "air_pollution",
                "data": data,
                "http_status": status,
                "response_time_ms": int(response_time)
            }
        return None

    def extract_openmeteo_current(self) -> Optional[Dict]:
        """Extract current weather from Open-Meteo (no API key needed)."""
        if not CONFIG.OPENMETEO_ENABLED:
            return None

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": CONFIG.LAT,
            "longitude": CONFIG.LON,
            "current": "temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,rain,showers,snowfall,weather_code,cloud_cover,pressure_msl,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m",
            "timezone": "Asia/Karachi"
        }

        data, status, response_time = self._make_request(url, params, "openmeteo_current")

        if data:
            return {
                "source": "openmeteo",
                "api_endpoint": "forecast",
                "data": data,
                "http_status": status,
                "response_time_ms": int(response_time)
            }
        return None

    def extract_openmeteo_hourly(self) -> Optional[Dict]:
        """Extract hourly forecast from Open-Meteo."""
        if not CONFIG.OPENMETEO_ENABLED:
            return None

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": CONFIG.LAT,
            "longitude": CONFIG.LON,
            "hourly": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation_probability,precipitation,rain,showers,snowfall,weather_code,cloud_cover,pressure_msl,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m,visibility",
            "forecast_days": 7,
            "timezone": "Asia/Karachi"
        }

        data, status, response_time = self._make_request(url, params, "openmeteo_hourly")

        if data:
            return {
                "source": "openmeteo",
                "api_endpoint": "forecast_hourly",
                "data": data,
                "http_status": status,
                "response_time_ms": int(response_time)
            }
        return None

    def extract_all(self) -> List[Dict]:
        """Extract from all available sources."""
        results = []

        # OpenWeatherMap sources
        current = self.extract_openweather_current()
        if current:
            results.append(current)

        forecast = self.extract_openweather_forecast()
        if forecast:
            results.append(forecast)

        air_quality = self.extract_openweather_air_quality()
        if air_quality:
            results.append(air_quality)

        # Open-Meteo sources (free, no key)
        openmeteo_current = self.extract_openmeteo_current()
        if openmeteo_current:
            results.append(openmeteo_current)

        openmeteo_hourly = self.extract_openmeteo_hourly()
        if openmeteo_hourly:
            results.append(openmeteo_hourly)

        logger.info(f"Extracted {len(results)} datasets from {len(set(r['source'] for r in results))} sources")
        return results

# ============================================================
# LANDING / RAW STORAGE
# ============================================================

class RawDataManager:
    """Manages raw data landing with partitioning and deduplication."""

    def __init__(self, redis_client=None):
        self.redis = redis_client

    def _generate_fingerprint(self, data: Dict) -> str:
        """Generate unique fingerprint for deduplication."""
        content = json.dumps(data["data"], sort_keys=True)
        return hashlib.md5(f"{data['source']}:{data['api_endpoint']}:{content}".encode()).hexdigest()

    def is_duplicate(self, fingerprint: str) -> bool:
        """Check if this exact data was already ingested."""
        if self.redis:
            return self.redis.exists(f"weather:fingerprint:{fingerprint}") == 1
        return False

    def mark_ingested(self, fingerprint: str, ttl: int = 86400):
        """Mark data as ingested with TTL."""
        if self.redis:
            self.redis.setex(f"weather:fingerprint:{fingerprint}", ttl, "1")

    def save_to_disk(self, data: Dict) -> str:
        """Save raw JSON to date-partitioned directory."""
        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        timestamp_str = now.strftime("%Y%m%d_%H%M%S")
        source = data["source"]
        endpoint = data["api_endpoint"]

        date_dir = CONFIG.RAW_DATA_DIR / date_str / source
        date_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{source}_{endpoint}_{timestamp_str}.json"
        filepath = date_dir / filename

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2, default=str)

        logger.info(f"Raw data saved: {filepath}")
        return str(filepath)

    def save_to_database(self, conn, data: Dict) -> str:
        """Save raw JSON to PostgreSQL raw layer."""
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO raw_weather (fetched_at, source, api_endpoint, city, country, 
                                    latitude, longitude, raw_json, http_status, response_time_ms)
            VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            data["source"],
            data["api_endpoint"],
            CONFIG.CITY,
            CONFIG.COUNTRY,
            CONFIG.LAT,
            CONFIG.LON,
            Json(data["data"]),
            data.get("http_status"),
            data.get("response_time_ms")
        ))

        raw_id = cursor.fetchone()[0]
        conn.commit()
        return str(raw_id)

# ============================================================
# TRANSFORMATION
# ============================================================

class WeatherTransformer:
    """Transforms raw API data into structured staging records."""

    @staticmethod
    def transform_openweather_current(raw_id: str, data: Dict, fetched_at: datetime) -> Optional[Dict]:
        """Transform OpenWeatherMap current weather data."""
        main = data.get("main", {})
        wind = data.get("wind", {})
        weather_list = data.get("weather", [{}])
        weather = weather_list[0] if weather_list else {}
        clouds = data.get("clouds", {})
        sys = data.get("sys", {})
        coord = data.get("coord", {})
        rain = data.get("rain", {})
        snow = data.get("snow", {})

        # Convert Unix timestamps
        sunrise = None
        sunset = None
        if sys.get("sunrise"):
            sunrise = datetime.fromtimestamp(sys["sunrise"], tz=timezone.utc)
        if sys.get("sunset"):
            sunset = datetime.fromtimestamp(sys["sunset"], tz=timezone.utc)

        record = {
            "raw_id": raw_id,
            "fetched_at": fetched_at,
            "source": "openweather",
            "city": data.get("name", CONFIG.CITY),
            "country": sys.get("country", CONFIG.COUNTRY),
            "latitude": coord.get("lat", CONFIG.LAT),
            "longitude": coord.get("lon", CONFIG.LON),
            "temperature": main.get("temp"),
            "feels_like": main.get("feels_like"),
            "temp_min": main.get("temp_min"),
            "temp_max": main.get("temp_max"),
            "pressure_hpa": main.get("pressure"),
            "humidity_percent": main.get("humidity"),
            "visibility_m": data.get("visibility"),
            "wind_speed_ms": wind.get("speed"),
            "wind_deg": wind.get("deg"),
            "wind_gust_ms": wind.get("gust"),
            "weather_main": weather.get("main"),
            "weather_description": weather.get("description"),
            "weather_icon": weather.get("icon"),
            "clouds_percent": clouds.get("all"),
            "rain_1h_mm": rain.get("1h"),
            "rain_3h_mm": rain.get("3h"),
            "snow_1h_mm": snow.get("1h"),
            "snow_3h_mm": snow.get("3h"),
            "sunrise_at": sunrise,
            "sunset_at": sunset,
            "timezone_offset_sec": data.get("timezone"),
        }

        record["data_quality_score"] = DataQualityChecker.calculate_quality_score(record)
        return record

    @staticmethod
    def transform_openweather_forecast(raw_id: str, data: Dict) -> List[Dict]:
        """Transform OpenWeatherMap forecast data into multiple records."""
        records = []
        city = data.get("city", {})

        for item in data.get("list", []):
            main = item.get("main", {})
            weather_list = item.get("weather", [{}])
            weather = weather_list[0] if weather_list else {}
            wind = item.get("wind", {})
            clouds = item.get("clouds", {})
            rain = item.get("rain", {})

            forecast_time = datetime.fromtimestamp(item.get("dt", 0), tz=timezone.utc)
            fetched_at = datetime.now(timezone.utc)
            hours_ahead = int((forecast_time - fetched_at).total_seconds() / 3600)

            records.append({
                "raw_id": raw_id,
                "fetched_at": fetched_at,
                "forecast_for": forecast_time,
                "forecast_hours_ahead": hours_ahead,
                "temperature": main.get("temp"),
                "feels_like": main.get("feels_like"),
                "temp_min": main.get("temp_min"),
                "temp_max": main.get("temp_max"),
                "pressure_hpa": main.get("pressure"),
                "humidity_percent": main.get("humidity"),
                "weather_main": weather.get("main"),
                "weather_description": weather.get("description"),
                "wind_speed_ms": wind.get("speed"),
                "wind_deg": wind.get("deg"),
                "clouds_percent": clouds.get("all"),
                "pop_percent": item.get("pop", 0) * 100 if item.get("pop") else None,
                "rain_mm": rain.get("3h"),
            })

        return records

    @staticmethod
    def transform_openweather_air_quality(raw_id: str, data: Dict) -> Optional[Dict]:
        """Transform OpenWeatherMap air quality data."""
        list_data = data.get("list", [{}])
        if not list_data:
            return None

        item = list_data[0]
        components = item.get("components", {})

        return {
            "raw_id": raw_id,
            "fetched_at": datetime.now(timezone.utc),
            "aqi": item.get("main", {}).get("aqi"),
            "co": components.get("co"),
            "no": components.get("no"),
            "no2": components.get("no2"),
            "o3": components.get("o3"),
            "so2": components.get("so2"),
            "pm2_5": components.get("pm2_5"),
            "pm10": components.get("pm10"),
            "nh3": components.get("nh3"),
        }

    @staticmethod
    def transform_openmeteo_current(raw_id: str, data: Dict) -> Optional[Dict]:
        """Transform Open-Meteo current weather data."""
        current = data.get("current", {})

        return {
            "raw_id": raw_id,
            "fetched_at": datetime.now(timezone.utc),
            "source": "openmeteo",
            "city": CONFIG.CITY,
            "country": CONFIG.COUNTRY,
            "latitude": data.get("latitude", CONFIG.LAT),
            "longitude": data.get("longitude", CONFIG.LON),
            "temperature": current.get("temperature_2m"),
            "feels_like": current.get("apparent_temperature"),
            "temp_min": None,
            "temp_max": None,
            "pressure_hpa": current.get("pressure_msl"),
            "humidity_percent": current.get("relative_humidity_2m"),
            "visibility_m": None,
            "wind_speed_ms": current.get("wind_speed_10m"),
            "wind_deg": current.get("wind_direction_10m"),
            "wind_gust_ms": current.get("wind_gusts_10m"),
            "weather_main": None,
            "weather_description": f"WMO code {current.get('weather_code')}",
            "weather_icon": None,
            "clouds_percent": current.get("cloud_cover"),
            "rain_1h_mm": current.get("rain"),
            "rain_3h_mm": None,
            "snow_1h_mm": current.get("snowfall"),
            "snow_3h_mm": None,
            "sunrise_at": None,
            "sunset_at": None,
            "timezone_offset_sec": None,
        }

    @staticmethod
    def transform_openmeteo_hourly(raw_id: str, data: Dict) -> List[Dict]:
        """Transform Open-Meteo hourly forecast data."""
        records = []
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])

        for i, t in enumerate(times):
            try:
                forecast_time = datetime.fromisoformat(t.replace("Z", "+00:00"))
            except:
                continue

            fetched_at = datetime.now(timezone.utc)
            hours_ahead = int((forecast_time - fetched_at).total_seconds() / 3600)

            records.append({
                "raw_id": raw_id,
                "fetched_at": fetched_at,
                "forecast_for": forecast_time,
                "forecast_hours_ahead": hours_ahead,
                "temperature": hourly.get("temperature_2m", [])[i] if i < len(hourly.get("temperature_2m", [])) else None,
                "feels_like": hourly.get("apparent_temperature", [])[i] if i < len(hourly.get("apparent_temperature", [])) else None,
                "temp_min": None,
                "temp_max": None,
                "pressure_hpa": hourly.get("pressure_msl", [])[i] if i < len(hourly.get("pressure_msl", [])) else None,
                "humidity_percent": hourly.get("relative_humidity_2m", [])[i] if i < len(hourly.get("relative_humidity_2m", [])) else None,
                "weather_main": f"WMO code {hourly.get('weather_code', [])[i]}" if i < len(hourly.get("weather_code", [])) else None,
                "weather_description": None,
                "wind_speed_ms": hourly.get("wind_speed_10m", [])[i] if i < len(hourly.get("wind_speed_10m", [])) else None,
                "wind_deg": hourly.get("wind_direction_10m", [])[i] if i < len(hourly.get("wind_direction_10m", [])) else None,
                "clouds_percent": hourly.get("cloud_cover", [])[i] if i < len(hourly.get("cloud_cover", [])) else None,
                "pop_percent": hourly.get("precipitation_probability", [])[i] if i < len(hourly.get("precipitation_probability", [])) else None,
                "rain_mm": hourly.get("precipitation", [])[i] if i < len(hourly.get("precipitation", [])) else None,
            })

        return records

# ============================================================
# LOADING
# ============================================================

class DataLoader:
    """Loads transformed data into staging tables."""

    def __init__(self, conn):
        self.conn = conn

    def load_current_weather(self, record: Dict) -> bool:
        """Load current weather record to staging."""
        cursor = self.conn.cursor()

        try:
            cursor.execute("""
                INSERT INTO stg_weather_current (
                    raw_id, fetched_at, source, city, country, latitude, longitude,
                    temperature, feels_like, temp_min, temp_max, pressure_hpa, humidity_percent,
                    visibility_m, wind_speed_ms, wind_deg, wind_gust_ms,
                    weather_main, weather_description, weather_icon, clouds_percent,
                    rain_1h_mm, rain_3h_mm, snow_1h_mm, snow_3h_mm,
                    sunrise_at, sunset_at, timezone_offset_sec, data_quality_score
                ) VALUES (
                    %(raw_id)s, %(fetched_at)s, %(source)s, %(city)s, %(country)s, 
                    %(latitude)s, %(longitude)s,
                    %(temperature)s, %(feels_like)s, %(temp_min)s, %(temp_max)s, 
                    %(pressure_hpa)s, %(humidity_percent)s,
                    %(visibility_m)s, %(wind_speed_ms)s, %(wind_deg)s, %(wind_gust_ms)s,
                    %(weather_main)s, %(weather_description)s, %(weather_icon)s, %(clouds_percent)s,
                    %(rain_1h_mm)s, %(rain_3h_mm)s, %(snow_1h_mm)s, %(snow_3h_mm)s,
                    %(sunrise_at)s, %(sunset_at)s, %(timezone_offset_sec)s, %(data_quality_score)s
                )
            """, record)

            self.conn.commit()
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to load current weather: {e}")
            self.conn.rollback()
            return False

    def load_forecast_batch(self, records: List[Dict]) -> int:
        """Batch load forecast records."""
        if not records:
            return 0

        cursor = self.conn.cursor()

        try:
            execute_values(cursor, """
                INSERT INTO stg_weather_forecast (
                    raw_id, fetched_at, forecast_for, forecast_hours_ahead,
                    temperature, feels_like, temp_min, temp_max, pressure_hpa,
                    humidity_percent, weather_main, weather_description,
                    wind_speed_ms, wind_deg, clouds_percent, pop_percent, rain_mm
                ) VALUES %s
            """, [
                (
                    r["raw_id"], r["fetched_at"], r["forecast_for"], r["forecast_hours_ahead"],
                    r["temperature"], r["feels_like"], r["temp_min"], r["temp_max"], r["pressure_hpa"],
                    r["humidity_percent"], r["weather_main"], r["weather_description"],
                    r["wind_speed_ms"], r["wind_deg"], r["clouds_percent"], r["pop_percent"], r["rain_mm"]
                )
                for r in records
            ])

            self.conn.commit()
            return len(records)
        except psycopg2.Error as e:
            logger.error(f"Failed to load forecast batch: {e}")
            self.conn.rollback()
            return 0

    def load_air_quality(self, record: Dict) -> bool:
        """Load air quality record."""
        cursor = self.conn.cursor()

        try:
            cursor.execute("""
                INSERT INTO stg_air_quality (
                    raw_id, fetched_at, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3
                ) VALUES (
                    %(raw_id)s, %(fetched_at)s, %(aqi)s, %(co)s, %(no)s, %(no2)s,
                    %(o3)s, %(so2)s, %(pm2_5)s, %(pm10)s, %(nh3)s
                )
            """, record)

            self.conn.commit()
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to load air quality: {e}")
            self.conn.rollback()
            return False

# ============================================================
# MAIN PIPELINE
# ============================================================

def run_pipeline():
    """Execute the full ETL pipeline."""
    logger.info("=" * 70)
    logger.info("ADVANCED WEATHER ETL PIPELINE - Starting")
    logger.info("=" * 70)

    conn = None
    tracker = None

    try:
        # Initialize connections
        conn = get_db_connection()
        redis_client = get_redis_client()
        tracker = PipelineTracker(conn)
        tracker.start_run("weather_etl_full", "full")

        # Initialize components
        extractor = WeatherExtractor(redis_client)
        raw_manager = RawDataManager(redis_client)
        transformer = WeatherTransformer()
        loader = DataLoader(conn)

        # Step 1: EXTRACT
        logger.info("--- EXTRACTION PHASE ---")
        extracted_data = extractor.extract_all()
        tracker.records_extracted = len(extracted_data)

        if not extracted_data:
            raise Exception("No data extracted from any source")

        # Step 2: LAND RAW DATA
        logger.info("--- RAW LANDING PHASE ---")
        raw_ids = []
        for data in extracted_data:
            # Deduplication check
            fingerprint = raw_manager._generate_fingerprint(data)
            if raw_manager.is_duplicate(fingerprint):
                logger.info(f"Skipping duplicate data from {data['source']}/{data['api_endpoint']}")
                continue

            # Save to disk
            raw_manager.save_to_disk(data)

            # Save to database raw layer
            raw_id = raw_manager.save_to_database(conn, data)
            raw_ids.append((raw_id, data))
            raw_manager.mark_ingested(fingerprint)

            logger.info(f"Landed raw data: {data['source']}/{data['api_endpoint']} -> {raw_id}")

        # Step 3: TRANSFORM & LOAD
        logger.info("--- TRANSFORMATION & LOADING PHASE ---")
        total_loaded = 0

        for raw_id, data in raw_ids:
            source = data["source"]
            endpoint = data["api_endpoint"]
            api_data = data["data"]
            fetched_at = datetime.now(timezone.utc)

            try:
                if source == "openweather":
                    if endpoint == "current":
                        record = transformer.transform_openweather_current(raw_id, api_data, fetched_at)
                        if record:
                            # Data quality check
                            ok, msg = DataQualityChecker.check_temperature(record.get("temperature"))
                            if not ok:
                                DataQualityChecker.log_quality_issue(
                                    conn, "stg_weather_current", "out_of_range", "warning",
                                    msg, sample_data={"temperature": record.get("temperature")}
                                )

                            if loader.load_current_weather(record):
                                total_loaded += 1
                                tracker.records_transformed += 1

                    elif endpoint == "forecast":
                        records = transformer.transform_openweather_forecast(raw_id, api_data)
                        loaded = loader.load_forecast_batch(records)
                        total_loaded += loaded
                        tracker.records_transformed += loaded

                    elif endpoint == "air_pollution":
                        record = transformer.transform_openweather_air_quality(raw_id, api_data)
                        if record and loader.load_air_quality(record):
                            total_loaded += 1
                            tracker.records_transformed += 1

                elif source == "openmeteo":
                    if endpoint == "forecast":
                        record = transformer.transform_openmeteo_current(raw_id, api_data)
                        if record:
                            record["data_quality_score"] = DataQualityChecker.calculate_quality_score(record)
                            if loader.load_current_weather(record):
                                total_loaded += 1
                                tracker.records_transformed += 1

                    elif endpoint == "forecast_hourly":
                        records = transformer.transform_openmeteo_hourly(raw_id, api_data)
                        loaded = loader.load_forecast_batch(records)
                        total_loaded += loaded
                        tracker.records_transformed += loaded

            except Exception as e:
                logger.error(f"Error processing {source}/{endpoint}: {e}")
                DataQualityChecker.log_quality_issue(
                    conn, "stg_weather_current", "transform_error", "error",
                    str(e), sample_data={"source": source, "endpoint": endpoint}
                )

        tracker.records_loaded = total_loaded

        # Mark raw records as processed
        cursor = conn.cursor()
        for raw_id, _ in raw_ids:
            cursor.execute("UPDATE raw_weather SET processed = TRUE WHERE id = %s", (raw_id,))
        conn.commit()

        # Step 4: Build marts (daily aggregation)
        logger.info("--- MART BUILDING PHASE ---")
        build_daily_mart(conn)

        tracker.update_status("success")

        logger.info("=" * 70)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Extracted: {tracker.records_extracted} | Transformed: {tracker.records_transformed} | Loaded: {tracker.records_loaded}")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        if tracker:
            tracker.update_status("failed", str(e))
        raise
    finally:
        if conn:
            conn.close()


def build_daily_mart(conn):
    """Build daily aggregation mart from staging data."""
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO mart_daily_weather (
            date, city, readings_count, avg_temperature, min_temperature, max_temperature,
            avg_feels_like, avg_humidity, min_humidity, max_humidity,
            avg_pressure, min_pressure, max_pressure,
            avg_wind_speed, max_wind_speed, max_wind_gust,
            total_rain_mm, total_snow_mm, dominant_weather, weather_conditions,
            sunrise_at, sunset_at, daylight_hours, data_completeness_score
        )
        SELECT 
            s.ingestion_date as date,
            'Bahawalpur' as city,
            COUNT(*) as readings_count,
            ROUND(AVG(temperature)::numeric, 2) as avg_temperature,
            MIN(temperature) as min_temperature,
            MAX(temperature) as max_temperature,
            ROUND(AVG(feels_like)::numeric, 2) as avg_feels_like,
            ROUND(AVG(humidity_percent))::int as avg_humidity,
            MIN(humidity_percent) as min_humidity,
            MAX(humidity_percent) as max_humidity,
            ROUND(AVG(pressure_hpa))::int as avg_pressure,
            MIN(pressure_hpa) as min_pressure,
            MAX(pressure_hpa) as max_pressure,
            ROUND(AVG(wind_speed_ms)::numeric, 2) as avg_wind_speed,
            MAX(wind_speed_ms) as max_wind_speed,
            MAX(wind_gust_ms) as max_wind_gust,
            SUM(COALESCE(rain_1h_mm, 0) + COALESCE(rain_3h_mm, 0)) as total_rain_mm,
            SUM(COALESCE(snow_1h_mm, 0) + COALESCE(snow_3h_mm, 0)) as total_snow_mm,
            MODE() WITHIN GROUP (ORDER BY weather_main) as dominant_weather,
            jsonb_object_agg(weather_main, cnt) as weather_conditions,
            (SELECT MIN(sunrise_at)::time FROM stg_weather_current WHERE ingestion_date = s.ingestion_date AND sunrise_at IS NOT NULL) as sunrise_at,
            (SELECT MAX(sunset_at)::time FROM stg_weather_current WHERE ingestion_date = s.ingestion_date AND sunset_at IS NOT NULL) as sunset_at,
            NULL as daylight_hours,
            ROUND((COUNT(*) FILTER (WHERE temperature IS NOT NULL) * 100.0 / COUNT(*))::numeric, 2) as data_completeness_score
        FROM stg_weather_current s
        LEFT JOIN (
            SELECT weather_main, ingestion_date, COUNT(*) as cnt
            FROM stg_weather_current
            WHERE weather_main IS NOT NULL
            GROUP BY weather_main, ingestion_date
        ) wc ON wc.ingestion_date = s.ingestion_date AND wc.weather_main = s.weather_main
        WHERE s.ingestion_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY s.ingestion_date
        ON CONFLICT (date) DO UPDATE SET
            readings_count = EXCLUDED.readings_count,
            avg_temperature = EXCLUDED.avg_temperature,
            min_temperature = EXCLUDED.min_temperature,
            max_temperature = EXCLUDED.max_temperature,
            avg_feels_like = EXCLUDED.avg_feels_like,
            avg_humidity = EXCLUDED.avg_humidity,
            min_humidity = EXCLUDED.min_humidity,
            max_humidity = EXCLUDED.max_humidity,
            avg_pressure = EXCLUDED.avg_pressure,
            min_pressure = EXCLUDED.min_pressure,
            max_pressure = EXCLUDED.max_pressure,
            avg_wind_speed = EXCLUDED.avg_wind_speed,
            max_wind_speed = EXCLUDED.max_wind_speed,
            max_wind_gust = EXCLUDED.max_wind_gust,
            total_rain_mm = EXCLUDED.total_rain_mm,
            total_snow_mm = EXCLUDED.total_snow_mm,
            dominant_weather = EXCLUDED.dominant_weather,
            weather_conditions = EXCLUDED.weather_conditions,
            sunrise_at = EXCLUDED.sunrise_at,
            sunset_at = EXCLUDED.sunset_at,
            daylight_hours = EXCLUDED.daylight_hours,
            data_completeness_score = EXCLUDED.data_completeness_score,
            updated_at = NOW();
    """)

    conn.commit()
    logger.info("Daily mart refreshed")


if __name__ == "__main__":
    run_pipeline()
