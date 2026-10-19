-- Weather alerts detection
SELECT
    id,
    fetched_at,
    city,
    temperature,
    humidity_percent,
    wind_speed_ms,
    wind_gust_ms,
    total_rain_mm,
    CASE
        WHEN temperature >= 45 THEN 'critical'
        WHEN temperature >= 40 THEN 'high'
        WHEN temperature >= 35 THEN 'medium'
        ELSE 'low'
    END as heat_alert_severity,
    CASE
        WHEN wind_speed_ms >= 20 THEN 'critical'
        WHEN wind_speed_ms >= 15 THEN 'high'
        WHEN wind_speed_ms >= 10 THEN 'medium'
        ELSE 'low'
    END as wind_alert_severity,
    CASE
        WHEN total_rain_mm >= 50 THEN 'critical'
        WHEN total_rain_mm >= 25 THEN 'high'
        WHEN total_rain_mm >= 10 THEN 'medium'
        ELSE 'low'
    END as rain_alert_severity,
    CASE
        WHEN temperature >= 45 OR wind_speed_ms >= 20 OR total_rain_mm >= 50 THEN 'critical'
        WHEN temperature >= 40 OR wind_speed_ms >= 15 OR total_rain_mm >= 25 THEN 'high'
        WHEN temperature >= 35 OR wind_speed_ms >= 10 OR total_rain_mm >= 10 THEN 'medium'
        ELSE 'low'
    END as overall_alert_level
FROM {{ ref('stg_weather_current_clean') }}
WHERE temperature >= 35 OR wind_speed_ms >= 10 OR total_rain_mm >= 10
