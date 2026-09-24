from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

storage_account = "learning11778653"
silver_path = (
    f"abfss://weather-data@{storage_account}.dfs.core.windows.net/silver/weather"
)
gold_path = (
    f"abfss://weather-data@{storage_account}.dfs.core.windows.net/gold/daily_forecast"
)
gold_export_json_path = f"abfss://weather-data@{storage_account}.dfs.core.windows.net/gold/export_daily_summary"

# Load Silver Data
silver_df = spark.read.format("delta").load(silver_path)

enriched_df = (
    silver_df.withColumn("forecast_timestamp", F.to_timestamp("timestamp"))
    .withColumn("forecast_date", F.to_date("forecast_timestamp"))
    .withColumn("hour", F.hour("forecast_timestamp"))
)

# Aggregate Hourly Data into Daily Summaries
daily_summary_df = enriched_df.groupBy(
    "location", "latitude", "longitude", "timezone", "forecast_date"
).agg(
    F.round(F.min("temperature_c"), 1).alias("temp_min_c"),
    F.round(F.max("temperature_c"), 1).alias("temp_max_c"),
    F.round(F.avg("temperature_c"), 1).alias("temp_avg_c"),
    F.round(F.max("apparent_temperature_c"), 1).alias("apparent_temp_max_c"),
    F.round(F.sum("precipitation_mm"), 1).alias("total_precipitation_mm"),
    F.max("precipitation_probability").alias("max_precipitation_probability_pct"),
    F.round(F.max("wind_gusts_kmh"), 1).alias("max_wind_gust_kmh"),
    F.round(F.avg("wind_speed_kmh"), 1).alias("avg_wind_speed_kmh"),
    F.round(F.avg("cloud_cover"), 0).alias("avg_cloud_cover_pct"),
    F.round(F.max("uv_index"), 1).alias("max_uv_index"),
    F.max(
        F.when(F.col("hour").between(12, 18), F.col("weather_code")).otherwise(0)
    ).alias("peak_weather_code"),
)

# Map WMO Weather Codes to Human-Readable Conditions for the UI
wmo_condition = (
    F.when(F.col("peak_weather_code") == 0, "Clear sky")
    .when(F.col("peak_weather_code").isin(1, 2, 3), "Partly cloudy")
    .when(F.col("peak_weather_code").isin(45, 48), "Foggy")
    .when(F.col("peak_weather_code").isin(51, 53, 55, 56, 57), "Drizzle")
    .when(F.col("peak_weather_code").isin(61, 63, 65, 66, 67), "Rain")
    .when(F.col("peak_weather_code").isin(71, 73, 75, 77), "Snow")
    .when(F.col("peak_weather_code").isin(80, 81, 82), "Rain showers")
    .when(F.col("peak_weather_code").isin(85, 86), "Snow showers")
    .when(F.col("peak_weather_code").isin(95, 96, 99), "Thunderstorm")
    .otherwise("Cloudy")
)

# Business Logic: Destination "Outdoor Score" (0 to 100)
# Penalizes high rain probabilities, heavy rain, cold/excessive heat, and strong winds
outdoor_score_calc = (
    100
    - (F.col("max_precipitation_probability_pct") * 0.4)
    - (F.least(F.col("total_precipitation_mm") * 5, F.lit(30)))
    - (
        F.when(F.col("temp_max_c") < 15, (15 - F.col("temp_max_c")) * 2)
        .when(F.col("temp_max_c") > 30, (F.col("temp_max_c") - 30) * 3)
        .otherwise(0)
    )
    - (
        F.when(
            F.col("max_wind_gust_kmh") > 35, (F.col("max_wind_gust_kmh") - 35) * 0.8
        ).otherwise(0)
    )
)

gold_df = (
    daily_summary_df.withColumn("weather_condition", wmo_condition)
    .withColumn(
        "outdoor_score",
        F.greatest(F.lit(0), F.round(outdoor_score_calc, 0)).cast("int"),
    )
    .withColumn("processed_at", F.current_timestamp())
)

# Rank Destinations per Date to easily highlight the top pick in Vienna / Lower Austria
window_spec = Window.partitionBy("forecast_date").orderBy(F.desc("outdoor_score"))
gold_ranked_df = gold_df.withColumn("daily_rank", F.dense_rank().over(window_spec))

# Write to Gold Delta Table (Partitioned by date for fast analytical queries)
(
    gold_ranked_df.write.format("delta")
    .mode("overwrite")
    .partitionBy("forecast_date")
    .save(gold_path)
)

# Export Single Consolidated JSON for GitHub Actions / Frontend
# Coalescing to 1 file makes it effortless for your GitHub Action to download a single file
(
    gold_ranked_df.orderBy("forecast_date", "daily_rank")
    .coalesce(1)
    .write.mode("overwrite")
    .json(gold_export_json_path)
)
