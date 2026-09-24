from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

# Paths and Configuration
storage_account = "learning11778653"
raw_path = f"abfss://weather-data@{storage_account}.dfs.core.windows.net/raw/*.json"
bronze_path = (
    f"abfss://weather-data@{storage_account}.dfs.core.windows.net/bronze/weather"
)
silver_path = (
    f"abfss://weather-data@{storage_account}.dfs.core.windows.net/silver/weather"
)
gold_path = (
    f"abfss://weather-data@{storage_account}.dfs.core.windows.net/gold/daily_forecast"
)
gold_export_json_path = f"abfss://weather-data@{storage_account}.dfs.core.windows.net/gold/export_daily_summary"

# --- Processing Phase (Bronze & Silver) ---

raw_df = spark.read.json(raw_path)
raw_df.write.format("delta").mode("overwrite").save(bronze_path)

bronze_df = spark.read.format("delta").load(bronze_path)

hourly_df = bronze_df.select(
    F.col("destination.name").alias("location"),
    F.col("destination.latitude").alias("latitude"),
    F.col("destination.longitude").alias("longitude"),
    F.col("timezone").alias("timezone"),
    F.explode(
        F.arrays_zip(
            "hourly.time",
            "hourly.temperature_2m",
            "hourly.apparent_temperature",
            "hourly.precipitation",
            "hourly.precipitation_probability",
            "hourly.rain",
            "hourly.snowfall",
            "hourly.weather_code",
            "hourly.cloud_cover",
            "hourly.wind_speed_10m",
            "hourly.wind_gusts_10m",
            "hourly.uv_index",
        )
    ).alias("weather"),
).select(
    "location",
    "latitude",
    "longitude",
    "timezone",
    F.col("weather.time").alias("timestamp"),
    F.col("weather.temperature_2m").alias("temperature_c"),
    F.col("weather.apparent_temperature").alias("apparent_temperature_c"),
    F.col("weather.precipitation").alias("precipitation_mm"),
    F.col("weather.precipitation_probability").alias("precipitation_probability"),
    F.col("weather.rain").alias("rain_mm"),
    F.col("weather.snowfall").alias("snowfall_cm"),
    F.col("weather.weather_code").alias("weather_code"),
    F.col("weather.cloud_cover").alias("cloud_cover"),
    F.col("weather.wind_speed_10m").alias("wind_speed_kmh"),
    F.col("weather.wind_gusts_10m").alias("wind_gusts_kmh"),
    F.col("weather.uv_index").alias("uv_index"),
)

hourly_df.write.format("delta").mode("overwrite").save(silver_path)

# --- Calculation Phase (Gold) ---

silver_df = spark.read.format("delta").load(silver_path)

enriched_df = (
    silver_df.withColumn("forecast_timestamp", F.to_timestamp("timestamp"))
    .withColumn("forecast_date", F.to_date("forecast_timestamp"))
    .withColumn("hour", F.hour("forecast_timestamp"))
)

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

# Outdoor Score Calculation (0 to 100)
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

window_spec = Window.partitionBy("forecast_date").orderBy(F.desc("outdoor_score"))
gold_ranked_df = gold_df.withColumn("daily_rank", F.dense_rank().over(window_spec))

(
    gold_ranked_df.write.format("delta")
    .mode("overwrite")
    .partitionBy("forecast_date")
    .save(gold_path)
)

(
    gold_ranked_df.orderBy("forecast_date", "daily_rank")
    .coalesce(1)
    .write.mode("overwrite")
    .json(gold_export_json_path)
)
