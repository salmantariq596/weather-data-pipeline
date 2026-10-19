-- ============================================================
-- WEATHER DATA WAREHOUSE SCHEMA
-- Multi-source, partitioned, indexed for analytics
-- ============================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- For text search

-- ============================================================
-- RAW LAYER: Landing zone for all API data
-- ============================================================

CREATE TABLE IF NOT EXISTS raw_weather (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    source VARCHAR(50) NOT NULL,  -- 'openweather', 'openmeteo', 'air_quality'
    api_endpoint VARCHAR(200),
    city VARCHAR(100) NOT NULL DEFAULT 'Bahawalpur',
    country VARCHAR(10) DEFAULT 'PK',
    latitude DECIMAL(10, 7) NOT NULL DEFAULT 29.3956,
    longitude DECIMAL(10, 7) NOT NULL DEFAULT 71.6836,
    raw_json JSONB NOT NULL,
    http_status INT,
    response_time_ms INT,
    ingestion_date DATE NOT NULL DEFAULT CURRENT_DATE,
    processed BOOLEAN NOT NULL DEFAULT FALSE
) PARTITION BY RANGE (ingestion_date);

-- Create partitions for current and next month
CREATE TABLE IF NOT EXISTS raw_weather_2026_06 
    PARTITION OF raw_weather 
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE TABLE IF NOT EXISTS raw_weather_2026_07 
    PARTITION OF raw_weather 
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

CREATE TABLE IF NOT EXISTS raw_weather_2026_08 
    PARTITION OF raw_weather 
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

CREATE INDEX idx_raw_weather_source ON raw_weather(source);
CREATE INDEX idx_raw_weather_fetched_at ON raw_weather(fetched_at DESC);
CREATE INDEX idx_raw_weather_ingestion_date ON raw_weather(ingestion_date);
CREATE INDEX idx_raw_weather_processed ON raw_weather(processed) WHERE processed = FALSE;
CREATE INDEX idx_raw_weather_json_gin ON raw_weather USING GIN (raw_json);

-- ============================================================
-- STAGING LAYER: Cleaned, typed data
-- ============================================================

CREATE TABLE IF NOT EXISTS stg_weather_current (
    id SERIAL PRIMARY KEY,
    raw_id UUID NOT NULL REFERENCES raw_weather(id),
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL,
    source VARCHAR(50) NOT NULL,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(10),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    temperature DECIMAL(6, 2),
    feels_like DECIMAL(6, 2),
    temp_min DECIMAL(6, 2),
    temp_max DECIMAL(6, 2),
    pressure_hpa INT,
    humidity_percent INT,
    visibility_m INT,
    wind_speed_ms DECIMAL(6, 2),
    wind_deg INT,
    wind_gust_ms DECIMAL(6, 2),
    weather_main VARCHAR(50),
    weather_description VARCHAR(100),
    weather_icon VARCHAR(20),
    clouds_percent INT,
    rain_1h_mm DECIMAL(6, 2),
    rain_3h_mm DECIMAL(6, 2),
    snow_1h_mm DECIMAL(6, 2),
    snow_3h_mm DECIMAL(6, 2),
    sunrise_at TIMESTAMP WITH TIME ZONE,
    sunset_at TIMESTAMP WITH TIME ZONE,
    timezone_offset_sec INT,
    data_quality_score DECIMAL(3, 2),  -- 0.00 to 1.00
    ingestion_date DATE NOT NULL DEFAULT CURRENT_DATE
);

CREATE INDEX idx_stg_weather_fetched_at ON stg_weather_current(fetched_at DESC);
CREATE INDEX idx_stg_weather_city ON stg_weather_current(city);
CREATE INDEX idx_stg_weather_ingestion_date ON stg_weather_current(ingestion_date);
CREATE INDEX idx_stg_weather_source ON stg_weather_current(source);

-- ============================================================
-- STAGING: Air Quality Data
-- ============================================================

CREATE TABLE IF NOT EXISTS stg_air_quality (
    id SERIAL PRIMARY KEY,
    raw_id UUID NOT NULL REFERENCES raw_weather(id),
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL,
    aqi INT,  -- 1=Good, 2=Fair, 3=Moderate, 4=Poor, 5=Very Poor
    co DECIMAL(10, 4),       -- μg/m³
    no DECIMAL(10, 4),
    no2 DECIMAL(10, 4),
    o3 DECIMAL(10, 4),
    so2 DECIMAL(10, 4),
    pm2_5 DECIMAL(10, 4),
    pm10 DECIMAL(10, 4),
    nh3 DECIMAL(10, 4),
    ingestion_date DATE NOT NULL DEFAULT CURRENT_DATE
);

CREATE INDEX idx_stg_air_quality_fetched_at ON stg_air_quality(fetched_at DESC);
CREATE INDEX idx_stg_air_quality_aqi ON stg_air_quality(aqi);

-- ============================================================
-- STAGING: Forecast Data
-- ============================================================

CREATE TABLE IF NOT EXISTS stg_weather_forecast (
    id SERIAL PRIMARY KEY,
    raw_id UUID NOT NULL REFERENCES raw_weather(id),
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL,
    forecast_for TIMESTAMP WITH TIME ZONE NOT NULL,
    forecast_hours_ahead INT,
    temperature DECIMAL(6, 2),
    feels_like DECIMAL(6, 2),
    temp_min DECIMAL(6, 2),
    temp_max DECIMAL(6, 2),
    pressure_hpa INT,
    humidity_percent INT,
    weather_main VARCHAR(50),
    weather_description VARCHAR(100),
    wind_speed_ms DECIMAL(6, 2),
    wind_deg INT,
    clouds_percent INT,
    pop_percent INT,  -- Probability of precipitation
    rain_mm DECIMAL(6, 2),
    ingestion_date DATE NOT NULL DEFAULT CURRENT_DATE
);

CREATE INDEX idx_stg_forecast_forecast_for ON stg_weather_forecast(forecast_for);
CREATE INDEX idx_stg_forecast_fetched_at ON stg_weather_forecast(fetched_at DESC);

-- ============================================================
-- MARTS: Daily Aggregations (Analytics-ready)
-- ============================================================

CREATE TABLE IF NOT EXISTS mart_daily_weather (
    date DATE PRIMARY KEY,
    city VARCHAR(100) NOT NULL DEFAULT 'Bahawalpur',
    readings_count INT NOT NULL DEFAULT 0,
    avg_temperature DECIMAL(6, 2),
    min_temperature DECIMAL(6, 2),
    max_temperature DECIMAL(6, 2),
    avg_feels_like DECIMAL(6, 2),
    avg_humidity INT,
    min_humidity INT,
    max_humidity INT,
    avg_pressure INT,
    min_pressure INT,
    max_pressure INT,
    avg_wind_speed DECIMAL(6, 2),
    max_wind_speed DECIMAL(6, 2),
    max_wind_gust DECIMAL(6, 2),
    total_rain_mm DECIMAL(6, 2),
    total_snow_mm DECIMAL(6, 2),
    dominant_weather VARCHAR(50),  -- Most frequent condition
    weather_conditions JSONB,      -- All conditions with counts
    avg_aqi INT,
    max_aqi INT,
    avg_pm2_5 DECIMAL(10, 4),
    avg_pm10 DECIMAL(10, 4),
    sunrise_at TIME,
    sunset_at TIME,
    daylight_hours DECIMAL(4, 2),
    data_completeness_score DECIMAL(5, 2),  -- Percentage
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_mart_daily_date ON mart_daily_weather(date DESC);

-- ============================================================
-- MARTS: Hourly Summary
-- ============================================================

CREATE TABLE IF NOT EXISTS mart_hourly_weather (
    hour TIMESTAMP WITH TIME ZONE PRIMARY KEY,
    city VARCHAR(100) NOT NULL DEFAULT 'Bahawalpur',
    avg_temperature DECIMAL(6, 2),
    avg_humidity INT,
    avg_pressure INT,
    avg_wind_speed DECIMAL(6, 2),
    weather_main VARCHAR(50),
    aqi INT,
    pm2_5 DECIMAL(10, 4),
    readings_count INT DEFAULT 0
);

CREATE INDEX idx_mart_hourly_hour ON mart_hourly_weather(hour DESC);

-- ============================================================
-- MARTS: Weather Alerts & Anomalies
-- ============================================================

CREATE TABLE IF NOT EXISTS mart_weather_alerts (
    id SERIAL PRIMARY KEY,
    detected_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    alert_type VARCHAR(50) NOT NULL,  -- 'heat_wave', 'cold_snap', 'high_wind', 'heavy_rain', 'poor_air'
    severity VARCHAR(20) NOT NULL,    -- 'low', 'medium', 'high', 'critical'
    description TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    metric_value DECIMAL(10, 2),
    threshold_value DECIMAL(10, 2),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    resolved_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_alerts_active ON mart_weather_alerts(is_active) WHERE is_active = TRUE;
CREATE INDEX idx_alerts_type ON mart_weather_alerts(alert_type);

-- ============================================================
-- PIPELINE METADATA: Track runs, errors, lineage
-- ============================================================

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_name VARCHAR(100) NOT NULL,
    run_type VARCHAR(50) NOT NULL,  -- 'full', 'incremental', 'backfill'
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) NOT NULL DEFAULT 'running',  -- 'running', 'success', 'failed', 'partial'
    records_extracted INT DEFAULT 0,
    records_transformed INT DEFAULT 0,
    records_loaded INT DEFAULT 0,
    error_message TEXT,
    duration_seconds INT
);

CREATE INDEX idx_pipeline_runs_started ON pipeline_runs(started_at DESC);
CREATE INDEX idx_pipeline_runs_status ON pipeline_runs(status);

-- ============================================================
-- DATA QUALITY: Track issues
-- ============================================================

CREATE TABLE IF NOT EXISTS data_quality_issues (
    id SERIAL PRIMARY KEY,
    detected_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    table_name VARCHAR(100) NOT NULL,
    issue_type VARCHAR(50) NOT NULL,  -- 'null_value', 'out_of_range', 'duplicate', 'schema_drift'
    severity VARCHAR(20) NOT NULL,      -- 'warning', 'error', 'critical'
    description TEXT NOT NULL,
    affected_rows INT,
    sample_data JSONB,
    is_resolved BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at TIMESTAMP WITH TIME ZONE
);

-- ============================================================
-- FUNCTIONS & TRIGGERS
-- ============================================================

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_mart_daily_weather_updated_at
    BEFORE UPDATE ON mart_daily_weather
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- VIEWS: Common analytics patterns
-- ============================================================

-- Temperature trends view
CREATE OR REPLACE VIEW vw_temperature_trends AS
SELECT 
    date,
    avg_temperature,
    min_temperature,
    max_temperature,
    max_temperature - min_temperature as diurnal_range,
    LAG(avg_temperature, 1) OVER (ORDER BY date) as prev_day_avg,
    avg_temperature - LAG(avg_temperature, 1) OVER (ORDER BY date) as temp_change
FROM mart_daily_weather
ORDER BY date DESC;

-- Air quality summary
CREATE OR REPLACE VIEW vw_air_quality_summary AS
SELECT 
    date_trunc('day', fetched_at)::date as date,
    AVG(aqi)::int as avg_aqi,
    MAX(aqi) as max_aqi,
    AVG(pm2_5) as avg_pm2_5,
    AVG(pm10) as avg_pm10,
    CASE 
        WHEN AVG(aqi) <= 1 THEN 'Good'
        WHEN AVG(aqi) <= 2 THEN 'Fair'
        WHEN AVG(aqi) <= 3 THEN 'Moderate'
        WHEN AVG(aqi) <= 4 THEN 'Poor'
        ELSE 'Very Poor'
    END as air_quality_label
FROM stg_air_quality
GROUP BY date_trunc('day', fetched_at)::date
ORDER BY date DESC;
