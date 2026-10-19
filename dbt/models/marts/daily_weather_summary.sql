-- Daily weather summary mart
WITH daily_stats AS (
    SELECT
        ingestion_date as date,
        source,
        COUNT(*) as readings_count,
        AVG(temperature) as avg_temperature,
        MIN(temperature) as min_temperature,
        MAX(temperature) as max_temperature,
        AVG(feels_like) as avg_feels_like,
        AVG(pressure_hpa) as avg_pressure,
        MIN(pressure_hpa) as min_pressure,
        MAX(pressure_hpa) as max_pressure,
        AVG(humidity_percent) as avg_humidity,
        MIN(humidity_percent) as min_humidity,
        MAX(humidity_percent) as max_humidity,
        AVG(wind_speed_ms) as avg_wind_speed,
        MAX(wind_speed_ms) as max_wind_speed,
        MAX(wind_gust_ms) as max_wind_gust,
        SUM(total_rain_mm) as total_rain_mm,
        SUM(total_snow_mm) as total_snow_mm,
        MODE() WITHIN GROUP (ORDER BY weather_main) as dominant_weather
    FROM {{ ref('stg_weather_current_clean') }}
    GROUP BY ingestion_date, source
)

SELECT
    date,
    source,
    readings_count,
    ROUND(avg_temperature::numeric, 2) as avg_temperature,
    ROUND(min_temperature::numeric, 2) as min_temperature,
    ROUND(max_temperature::numeric, 2) as max_temperature,
    ROUND(avg_feels_like::numeric, 2) as avg_feels_like,
    ROUND(avg_pressure::numeric, 0) as avg_pressure,
    min_pressure,
    max_pressure,
    ROUND(avg_humidity::numeric, 0) as avg_humidity,
    min_humidity,
    max_humidity,
    ROUND(avg_wind_speed::numeric, 2) as avg_wind_speed,
    max_wind_speed,
    max_wind_gust,
    ROUND(total_rain_mm::numeric, 2) as total_rain_mm,
    ROUND(total_snow_mm::numeric, 2) as total_snow_mm,
    dominant_weather,
    -- Heat index approximation
    ROUND((avg_temperature + 0.5555 * (6.11 * EXP(5417.7530 * ((1/273.16) - (1/(273.15 + avg_temperature)))) - 10))::numeric, 2) as heat_index_approx,
    -- Comfort index
    CASE 
        WHEN avg_temperature BETWEEN 20 AND 26 AND avg_humidity BETWEEN 40 AND 60 THEN 'comfortable'
        WHEN avg_temperature > 30 OR avg_humidity > 70 THEN 'uncomfortable'
        ELSE 'moderate'
    END as comfort_index
FROM daily_stats
