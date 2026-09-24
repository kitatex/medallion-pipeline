from pyspark.sql import SparkSession

from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()

storage_account = "learning11778653"

raw_path = f"abfss://weather-data@{storage_account}.dfs.core.windows.net/raw/*.json"

bronze_path = (
    f"abfss://weather-data@{storage_account}.dfs.core.windows.net/bronze/weather"
)

silver_path = (
    f"abfss://weather-data@{storage_account}.dfs.core.windows.net/silver/weather"
)

# Bronze
raw_df = spark.read.json(raw_path)

(raw_df.write.format("delta").mode("overwrite").save(bronze_path))

# Silver
bronze_df = spark.read.format("delta").load(bronze_path)

hourly_df = bronze_df.select(
    "latitude",
    "longitude",
    "timezone",
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
    "latitude",
    "longitude",
    "timezone",
    F.col("weather.0").alias("timestamp"),
    F.col("weather.1").alias("temperature_c"),
    F.col("weather.2").alias("apparent_temperature_c"),
    F.col("weather.3").alias("precipitation_mm"),
    F.col("weather.4").alias("precipitation_probability"),
    F.col("weather.5").alias("rain_mm"),
    F.col("weather.6").alias("snowfall_cm"),
    F.col("weather.7").alias("weather_code"),
    F.col("weather.8").alias("cloud_cover"),
    F.col("weather.9").alias("wind_speed_kmh"),
    F.col("weather.10").alias("wind_gusts_kmh"),
    F.col("weather.11").alias("uv_index"),
)

(hourly_df.write.format("delta").mode("overwrite").save(silver_path))
