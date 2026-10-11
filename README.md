# 🌦️ Real-Time Weather Data ETL Pipeline

An **ETL pipeline** that extracts real-time weather data from the
**OpenWeatherMap REST API**, transforms the JSON response into structured
records, and loads it into a **MySQL** database for SQL-based trend analysis.

## 🎯 Objectives

- Extract weather data from a REST API
- Transform nested JSON into a clean, structured schema
- Load records into MySQL with validation & error handling
- Enable SQL analysis of temperature, humidity and weather trends

## ✨ Features

- 🌐 **REST API extraction** — live weather via OpenWeatherMap
- 🔧 **JSON transformation** — flattens nested fields into a table schema
- ✅ **Data validation** — drops missing/implausible readings (temp, humidity)
- ⏱️ **Timestamp-based processing** — unique on `(city, observed_at)`
- 🛡️ **Error handling** — graceful DEMO mode when no API key / DB is available
- 📉 **SQL analysis** — sample queries for temperature & humidity trends
- 🔁 **Idempotent loads** — `ON DUPLICATE KEY UPDATE` upserts

## 🧱 Tech Stack

`Python` · `REST APIs` · `Pandas` · `MySQL` · `SQL` · `GitHub`

## 🚀 Getting Started

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure a free API key (optional — DEMO mode works without it)
cp .env.example .env      # then add your OpenWeatherMap key

# 3. Run the pipeline
python weather_etl.py --city Visakhapatnam
```

> 💡 Without an API key or a running MySQL instance, the pipeline generates a
> realistic sample reading and writes it to `output/weather_readings.csv`, so it
> is always runnable end-to-end.

## 📂 Project Structure

```
Weather-ETL-Pipeline/
├── weather_etl.py     # Extract / Transform / Load logic
├── queries.sql        # Sample analytical queries
├── requirements.txt
├── .env.example
└── README.md
```

## 📈 Sample Analysis

```sql
-- Average temperature & humidity per day
SELECT DATE(observed_at) AS day,
       AVG(temperature_c) AS avg_temp,
       AVG(humidity_pct)  AS avg_humidity
FROM weather_readings
GROUP BY day
ORDER BY day;
```

---

Built by [Roopa Juttada](https://github.com/roopajuttada08) 🚀
