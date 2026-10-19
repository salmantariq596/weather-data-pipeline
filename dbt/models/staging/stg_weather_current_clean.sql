-- Cleaned current weather data from staging
SELECT
    id,
    raw_id,
    fetched_at,
    source,
    city,
    country,
    latitude,
    longitude,
    temperature,
    feels_like,
    temp_min,
    temp_max,
    pressure_hpa,
    humidity_percent,
    visibility_m,
    wind_speed_ms,
    wind_deg,
    wind_gust_ms,
    weather_main,
    weather_description,
    weather_icon,
    clouds_percent,
    COALESCE(rain_1h_mm, 0) + COALESCE(rain_3h_mm, 0) as total_rain_mm,
    COALESCE(snow_1h_mm, 0) + COALESCE(snow_3h_mm, 0) as total_snow_mm,
    sunrise_at,
    sunset_at,
    timezone_offset_sec,
    data_quality_score,
    ingestion_date,
    -- Derived metrics
    CASE 
        WHEN temperature >= 35 THEN 'extreme_heat'
        WHEN temperature >= 30 THEN 'hot'
        WHEN temperature >= 20 THEN 'warm'
        WHEN temperature >= 10 THEN 'mild'
        WHEN temperature >= 0 THEN 'cold'
        ELSE 'freezing'
    END as temp_category,
    CASE 
        WHEN humidity_percent >= 80 THEN 'very_humid'
        WHEN humidity_percent >= 60 THEN 'humid'
        WHEN humidity_percent >= 40 THEN 'moderate'
        ELSE 'dry'
    END as humidity_category,
    CASE 
        WHEN wind_speed_ms >= 20 THEN 'storm'
        WHEN wind_speed_ms >= 10 THEN 'strong'
        WHEN wind_speed_ms >= 5 THEN 'moderate'
        ELSE 'calm'
    END as wind_category
FROM stg_weather_current
WHERE data_quality_score >= 0.5
