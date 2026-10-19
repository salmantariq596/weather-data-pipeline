-- ============================================================
-- WEATHER DATA WAREHOUSE SCHEMA
-- Multi-source, partitioned, indexed for time-series analytics
-- ============================================================

-- 1. RAW WEATHER DATA (Bronze layer)
CREATE TABLE IF NOT EXISTS raw_weather (
    id BIGSERIAL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source VARCHAR(50) NOT NULL DEFAULT 'openweathermap',
    city VARCHAR(100) NOT NULL,
    country_code CHAR(2),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    temperature DECIMAL(6, 2),
    feels_like DECIMAL(6, 2),
    temp_min DECIMAL(6, 2),
    temp_max DECIMAL(6, 2),
    pressure INT,
    humidity INT,
    visibility INT,
    wind_speed DECIMAL(6, 2),
    wind_deg INT,
    wind_gust DECIMAL(6, 2),
    weather_main VARCHAR(50),
    weather_description VARCHAR(100),
    weather_icon VARCHAR(10),
    clouds INT,
    rain_1h DECIMAL(6, 2),
    rain_3h DECIMAL(6, 2),
    snow_1h DECIMAL(6, 2),
    snow_3h DECIMAL(6, 2),
    sunrise TIMESTAMPTZ,
    sunset TIMESTAMPTZ,
    timezone_offset INT,
    raw_json JSONB NOT NULL,
    ingestion_date DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (id, ingestion_date)
) PARTITION BY RANGE (ingestion_date);

-- Create monthly partitions
CREATE TABLE raw_weather_2026_01 PARTITION OF raw_weather
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE raw_weather_2026_02 PARTITION OF raw_weather
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE raw_weather_2026_03 PARTITION OF raw_weather
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE raw_weather_2026_04 PARTITION OF raw_weather
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE raw_weather_2026_05 PARTITION OF raw_weather
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE raw_weather_2026_06 PARTITION OF raw_weather
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE raw_weather_2026_07 PARTITION OF raw_weather
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE raw_weather_2026_08 PARTITION OF raw_weather
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE raw_weather_2026_09 PARTITION OF raw_weather
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE raw_weather_2026_10 PARTITION OF raw_weather
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE raw_weather_2026_11 PARTITION OF raw_weather
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE raw_weather_2026_12 PARTITION OF raw_weather
    FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');

-- Indexes
CREATE INDEX idx_raw_weather_ingestion ON raw_weather(ingestion_date);
CREATE INDEX idx_raw_weather_fetched ON raw_weather(fetched_at DESC);
CREATE INDEX idx_raw_weather_city ON raw_weather(city);
CREATE INDEX idx_raw_weather_json ON raw_weather USING GIN(raw_json);

-- 2. DAILY AGGREGATES (Silver layer)
CREATE TABLE IF NOT EXISTS daily_weather_stats (
    id SERIAL PRIMARY KEY,
    stat_date DATE NOT NULL UNIQUE,
    city VARCHAR(100) NOT NULL,
    readings_count INT NOT NULL DEFAULT 0,
    avg_temp DECIMAL(6, 2),
    min_temp DECIMAL(6, 2),
    max_temp DECIMAL(6, 2),
    avg_feels_like DECIMAL(6, 2),
    avg_humidity DECIMAL(5, 2),
    min_humidity INT,
    max_humidity INT,
    avg_pressure DECIMAL(7, 2),
    avg_wind_speed DECIMAL(6, 2),
    max_wind_speed DECIMAL(6, 2),
    total_rain DECIMAL(6, 2),
    total_snow DECIMAL(6, 2),
    dominant_weather VARCHAR(50),
    temp_range DECIMAL(6, 2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_daily_stats_date ON daily_weather_stats(stat_date DESC);

-- 3. HOURLY FORECAST DATA (from 5-day forecast API)
CREATE TABLE IF NOT EXISTS weather_forecast (
    id BIGSERIAL PRIMARY KEY,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    forecast_time TIMESTAMPTZ NOT NULL,
    city VARCHAR(100) NOT NULL,
    country_code CHAR(2),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    temperature DECIMAL(6, 2),
    feels_like DECIMAL(6, 2),
    temp_min DECIMAL(6, 2),
    temp_max DECIMAL(6, 2),
    pressure INT,
    humidity INT,
    visibility INT,
    wind_speed DECIMAL(6, 2),
    wind_deg INT,
    wind_gust DECIMAL(6, 2),
    weather_main VARCHAR(50),
    weather_description VARCHAR(100),
    clouds INT,
    rain_3h DECIMAL(6, 2),
    snow_3h DECIMAL(6, 2),
    pop DECIMAL(4, 2),  -- Probability of precipitation
    raw_json JSONB,
    ingestion_date DATE NOT NULL DEFAULT CURRENT_DATE
);

CREATE INDEX idx_forecast_time ON weather_forecast(forecast_time);
CREATE INDEX idx_forecast_ingestion ON weather_forecast(ingestion_date);

-- 4. AIR QUALITY DATA (from OpenWeatherMap Air Pollution API)
CREATE TABLE IF NOT EXISTS air_quality (
    id BIGSERIAL PRIMARY KEY,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    city VARCHAR(100) NOT NULL,
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    aqi INT,  -- 1=Good, 2=Fair, 3=Moderate, 4=Poor, 5=Very Poor
    co DECIMAL(10, 3),      -- Carbon monoxide
    no DECIMAL(10, 3),      -- Nitrogen monoxide
    no2 DECIMAL(10, 3),     -- Nitrogen dioxide
    o3 DECIMAL(10, 3),      -- Ozone
    so2 DECIMAL(10, 3),     -- Sulphur dioxide
    pm2_5 DECIMAL(10, 3),   -- Fine particles
    pm10 DECIMAL(10, 3),    -- Coarse particles
    nh3 DECIMAL(10, 3),     -- Ammonia
    raw_json JSONB,
    ingestion_date DATE NOT NULL DEFAULT CURRENT_DATE
);

CREATE INDEX idx_air_quality_date ON air_quality(ingestion_date);
CREATE INDEX idx_air_quality_aqi ON air_quality(aqi);

-- 5. UV INDEX DATA
CREATE TABLE IF NOT EXISTS uv_index (
    id BIGSERIAL PRIMARY KEY,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    city VARCHAR(100) NOT NULL,
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    uv_value DECIMAL(4, 2),
    uv_risk_level VARCHAR(20),  -- Low, Moderate, High, Very High, Extreme
    ingestion_date DATE NOT NULL DEFAULT CURRENT_DATE
);

-- 6. PIPELINE METRICS / AUDIT LOG
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    pipeline_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,  -- success, failed, partial
    records_processed INT DEFAULT 0,
    records_inserted INT DEFAULT 0,
    records_failed INT DEFAULT 0,
    duration_seconds DECIMAL(8, 3),
    error_message TEXT,
    raw_file_path VARCHAR(500),
    s3_path VARCHAR(500)
);

CREATE INDEX idx_pipeline_runs_time ON pipeline_runs(run_at DESC);
CREATE INDEX idx_pipeline_runs_status ON pipeline_runs(status);

-- 7. ALERTS / THRESHOLDS
CREATE TABLE IF NOT EXISTS weather_alerts (
    id SERIAL PRIMARY KEY,
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    alert_type VARCHAR(50) NOT NULL,  -- high_temp, low_temp, high_wind, heavy_rain, poor_air
    severity VARCHAR(20) NOT NULL,    -- info, warning, critical
    message TEXT NOT NULL,
    temperature DECIMAL(6, 2),
    wind_speed DECIMAL(6, 2),
    aqi INT,
    acknowledged BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_alerts_time ON weather_alerts(triggered_at DESC);

-- 8. MATERIALIZED VIEW: Weekly Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_weekly_summary AS
SELECT 
    DATE_TRUNC('week', ingestion_date) as week_start,
    city,
    COUNT(*) as total_readings,
    ROUND(AVG(temperature), 2) as avg_temp,
    MIN(temperature) as min_temp,
    MAX(temperature) as max_temp,
    ROUND(AVG(humidity), 1) as avg_humidity,
    ROUND(AVG(pressure), 1) as avg_pressure,
    ROUND(AVG(wind_speed), 2) as avg_wind_speed,
    MAX(wind_speed) as max_wind_speed,
    ROUND(SUM(COALESCE(rain_1h, 0) + COALESCE(rain_3h, 0)), 2) as total_rain
FROM raw_weather
GROUP BY DATE_TRUNC('week', ingestion_date), city;

CREATE UNIQUE INDEX idx_mv_weekly ON mv_weekly_summary(week_start, city);

-- Function to refresh materialized view
CREATE OR REPLACE FUNCTION refresh_weekly_summary()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_weekly_summary;
END;
$$ LANGUAGE plpgsql;
