"""
Advanced SQL Analysis Queries for Weather Data
"""

QUERIES = {
    "1. Temperature trends with rolling averages": """
        SELECT 
            date,
            avg_temperature,
            min_temperature,
            max_temperature,
            AVG(avg_temperature) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_7day_avg,
            avg_temperature - LAG(avg_temperature, 1) OVER (ORDER BY date) as day_over_day_change,
            avg_temperature - LAG(avg_temperature, 7) OVER (ORDER BY date) as week_over_week_change
        FROM mart_daily_weather
        ORDER BY date DESC
        LIMIT 30;
    """,

    "2. Heat wave detection (3+ consecutive days >= 40°C)": """
        WITH hot_days AS (
            SELECT date, max_temperature,
                CASE WHEN max_temperature >= 40 THEN 1 ELSE 0 END as is_hot
            FROM mart_daily_weather
        ),
        hot_streaks AS (
            SELECT date, max_temperature, is_hot,
                SUM(CASE WHEN is_hot = 0 THEN 1 ELSE 0 END) OVER (ORDER BY date) as streak_group
            FROM hot_days
        )
        SELECT streak_group, 
               MIN(date) as streak_start, 
               MAX(date) as streak_end,
               COUNT(*) as streak_length,
               MAX(max_temperature) as peak_temp
        FROM hot_streaks
        WHERE is_hot = 1
        GROUP BY streak_group
        HAVING COUNT(*) >= 3
        ORDER BY streak_start DESC;
    """,

    "3. Correlation analysis": """
        SELECT 
            ROUND(CORR(temperature, humidity_percent)::numeric, 3) as temp_humidity_corr,
            ROUND(CORR(temperature, pressure_hpa)::numeric, 3) as temp_pressure_corr,
            ROUND(CORR(humidity_percent, pressure_hpa)::numeric, 3) as humidity_pressure_corr,
            ROUND(CORR(wind_speed_ms, temperature)::numeric, 3) as wind_temp_corr
        FROM stg_weather_current
        WHERE fetched_at >= CURRENT_DATE - INTERVAL '30 days';
    """,

    "4. Data quality report": """
        SELECT 
            'stg_weather_current' as table_name,
            COUNT(*) as total_records,
            COUNT(*) FILTER (WHERE temperature IS NULL) as null_temperature,
            COUNT(*) FILTER (WHERE humidity_percent IS NULL) as null_humidity,
            COUNT(*) FILTER (WHERE pressure_hpa IS NULL) as null_pressure,
            COUNT(*) FILTER (WHERE data_quality_score < 0.5) as low_quality_records,
            ROUND(AVG(data_quality_score)::numeric, 3) as avg_quality_score,
            MIN(fetched_at) as earliest_record,
            MAX(fetched_at) as latest_record
        FROM stg_weather_current
        UNION ALL
        SELECT 
            'stg_air_quality',
            COUNT(*),
            COUNT(*) FILTER (WHERE aqi IS NULL),
            COUNT(*) FILTER (WHERE pm2_5 IS NULL),
            COUNT(*) FILTER (WHERE pm10 IS NULL),
            0,
            NULL,
            MIN(fetched_at),
            MAX(fetched_at)
        FROM stg_air_quality;
    """,

    "5. Pipeline health check": """
        SELECT 
            pipeline_name,
            run_type,
            status,
            started_at,
            completed_at,
            duration_seconds,
            records_extracted,
            records_transformed,
            records_loaded,
            error_message
        FROM pipeline_runs
        ORDER BY started_at DESC
        LIMIT 10;
    """,

    "6. Source comparison": """
        SELECT 
            source,
            COUNT(*) as total_readings,
            MIN(fetched_at) as first_reading,
            MAX(fetched_at) as latest_reading,
            ROUND(AVG(temperature)::numeric, 2) as avg_temp,
            ROUND(AVG(humidity_percent)::numeric, 0) as avg_humidity,
            ROUND(AVG(pressure_hpa)::numeric, 0) as avg_pressure
        FROM stg_weather_current
        GROUP BY source
        ORDER BY total_readings DESC;
    """,

    "7. Forecast accuracy (when you have actuals)": """
        SELECT 
            f.forecast_for::date as forecast_date,
            f.forecast_hours_ahead,
            ROUND(f.temperature::numeric, 2) as forecasted_temp,
            ROUND(a.temperature::numeric, 2) as actual_temp,
            ROUND(ABS(f.temperature - a.temperature)::numeric, 2) as temp_error,
            ROUND(ABS(f.temperature - a.temperature) / NULLIF(a.temperature, 0) * 100, 2) as temp_error_pct
        FROM stg_weather_forecast f
        JOIN stg_weather_current a 
            ON f.forecast_for::date = a.fetched_at::date 
            AND EXTRACT(HOUR FROM f.forecast_for) = EXTRACT(HOUR FROM a.fetched_at)
        WHERE f.forecast_hours_ahead <= 24
        ORDER BY f.forecast_for DESC
        LIMIT 50;
    """
}


def print_queries():
    """Print all analysis queries."""
    print("=" * 70)
    print("ADVANCED WEATHER DATA ANALYSIS QUERIES")
    print("=" * 70)
    print()

    for name, query in QUERIES.items():
        print(f"\n{name}")
        print("-" * 70)
        print(query)
        print()


if __name__ == "__main__":
    print_queries()
