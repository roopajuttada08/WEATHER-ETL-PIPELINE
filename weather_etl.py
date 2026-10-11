"""
Real-Time Weather Data ETL Pipeline
===================================
Extract  -> weather data from the OpenWeatherMap REST API
Transform-> flatten the JSON response into structured records
Load     -> into a MySQL database for SQL-based trend analysis

Run:
    set WEATHER_API_KEY=your_key        (Windows:  $env:WEATHER_API_KEY="...")
    python weather_etl.py --city Visakhapatnam

If no API key / MySQL is available, the pipeline runs in DEMO mode:
it generates a realistic sample payload and writes to CSV so it stays runnable.
"""
import argparse
import datetime as dt
import logging
import os

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("weather_etl")

API_KEY = os.getenv("WEATHER_API_KEY", "")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DB", "weather_db"),
}


# --------------------------------------------------------------------------- #
# EXTRACT
# --------------------------------------------------------------------------- #
def extract_weather(city: str) -> dict:
    """Call the OpenWeatherMap API. Falls back to a demo payload if no key."""
    if not API_KEY:
        logger.warning("No WEATHER_API_KEY set — using DEMO payload.")
        return _demo_payload(city)

    params = {"q": city, "appid": API_KEY, "units": "metric"}
    try:
        resp = requests.get(BASE_URL, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        logger.error("API extraction failed (%s) — using DEMO payload.", exc)
        return _demo_payload(city)


def _demo_payload(city: str) -> dict:
    """A realistic sample response so the pipeline is runnable offline."""
    now = dt.datetime.utcnow()
    return {
        "coord": {"lon": 83.3, "lat": 17.68},
        "weather": [{"main": "Clouds", "description": "scattered clouds"}],
        "main": {
            "temp": 29.4,
            "feels_like": 33.1,
            "humidity": 74,
            "pressure": 1008,
        },
        "wind": {"speed": 4.6},
        "name": city,
        "dt": int(now.timestamp()),
        "cod": 200,
    }


# --------------------------------------------------------------------------- #
# TRANSFORM
# --------------------------------------------------------------------------- #
def transform(payload: dict) -> pd.DataFrame:
    """Flatten the nested JSON into a single structured record."""
    try:
        record = {
            "city": payload.get("name"),
            "observed_at": dt.datetime.utcfromtimestamp(payload["dt"]).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "temperature_c": payload["main"]["temp"],
            "feels_like_c": payload["main"].get("feels_like"),
            "humidity_pct": payload["main"]["humidity"],
            "pressure_hpa": payload["main"].get("pressure"),
            "wind_speed_ms": payload.get("wind", {}).get("speed"),
            "condition": payload["weather"][0]["main"],
            "description": payload["weather"][0]["description"],
            "lat": payload["coord"]["lat"],
            "lon": payload["coord"]["lon"],
        }
    except (KeyError, IndexError, TypeError) as exc:
        logger.error("Transform failed due to unexpected payload: %s", exc)
        return pd.DataFrame()

    df = pd.DataFrame([record])

    # Validation: drop records missing essential fields or with bad values
    df = df.dropna(subset=["city", "temperature_c", "humidity_pct"])
    df = df[(df["temperature_c"] > -90) & (df["temperature_c"] < 60)]
    df = df[(df["humidity_pct"] >= 0) & (df["humidity_pct"] <= 100)]

    logger.info("Transformed 1 weather record for '%s'", record["city"])
    return df


# --------------------------------------------------------------------------- #
# LOAD
# --------------------------------------------------------------------------- #
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather_readings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(120),
    observed_at DATETIME,
    temperature_c DECIMAL(5,2),
    feels_like_c DECIMAL(5,2),
    humidity_pct INT,
    pressure_hpa INT,
    wind_speed_ms DECIMAL(6,2),
    condition VARCHAR(60),
    description VARCHAR(120),
    lat DECIMAL(9,6),
    lon DECIMAL(9,6),
    UNIQUE KEY uq_city_time (city, observed_at)
)
"""

INSERT_SQL = """
INSERT INTO weather_readings
    (city, observed_at, temperature_c, feels_like_c, humidity_pct,
     pressure_hpa, wind_speed_ms, condition, description, lat, lon)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE temperature_c = VALUES(temperature_c)
"""

COLUMNS = [
    "city", "observed_at", "temperature_c", "feels_like_c", "humidity_pct",
    "pressure_hpa", "wind_speed_ms", "condition", "description", "lat", "lon",
]


def load(df: pd.DataFrame) -> None:
    if df.empty:
        logger.warning("No data to load.")
        return
    rows = [tuple(r[c] for c in COLUMNS) for r in df.to_dict("records")]
    try:
        import mysql.connector
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(CREATE_TABLE_SQL)
        cur.executemany(INSERT_SQL, rows)
        conn.commit()
        logger.info("Loaded %d reading(s) into MySQL 'weather_readings'.", cur.rowcount)
        cur.close()
        conn.close()
    except Exception as exc:  # noqa: BLE001
        logger.warning("MySQL unavailable (%s). Writing CSV fallback.", exc)
        os.makedirs("output", exist_ok=True)
        df.to_csv("output/weather_readings.csv", index=False)
        logger.info("Exported reading(s) to output/weather_readings.csv")


# --------------------------------------------------------------------------- #
# ORCHESTRATION
# --------------------------------------------------------------------------- #
def run(city: str) -> None:
    logger.info("=== Weather ETL pipeline started for '%s' ===", city)
    payload = extract_weather(city)
    df = transform(payload)
    load(df)
    logger.info("=== Weather ETL pipeline finished ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Real-time weather ETL pipeline")
    parser.add_argument("--city", default="Visakhapatnam", help="City name")
    args = parser.parse_args()
    run(args.city)
