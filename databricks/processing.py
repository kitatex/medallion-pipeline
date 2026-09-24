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

(hourly_df.write.format("delta").mode("overwrite").save(silver_path))
