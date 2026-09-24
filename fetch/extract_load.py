import json

import requests
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

# Open-Meteo API (Extract)

DESTINATIONS = {
    "kahlenberg": (48.276342, 16.333068),
    "donaustadtbruecke": (48.210022, 16.436158),
    "lainzer_tiergarten": (48.176659, 16.220236),
    "neusiedl_am_see": (47.931078, 16.836808),
    "donau_auen": (48.130128, 16.739518),
    "oetschergraeben": (47.845040, 15.271387),
    "duernstein": (48.397793, 15.523046),
}

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "precipitation_probability",
    "rain",
    "snowfall",
    "weather_code",
    "cloud_cover",
    "wind_speed_10m",
    "wind_gusts_10m",
    "uv_index",
]

DAILY_VARIABLES = [
    "weather_code",
    "temperature_2m_max",
    "temperature_2m_min",
    "apparent_temperature_max",
    "apparent_temperature_min",
    "sunrise",
    "sunset",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "precipitation_probability_max",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "uv_index_max",
]


# Azure Data Lake (Load)
storage_account_name = "learning11778653"

account_url = f"https://{storage_account_name}.dfs.core.windows.net"
credential = DefaultAzureCredential()


service_client = DataLakeServiceClient(
    account_url=account_url,
    credential=credential,
)

file_system_client = service_client.get_file_system_client("weather-data")


for name, (latitude, longitude) in DESTINATIONS.items():
    print(f"Fetching weather data for {name}...")

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": HOURLY_VARIABLES,
        "daily": DAILY_VARIABLES,
        "forecast_days": 7,
        "timezone": "Europe/Vienna",
    }

    response = requests.get(OPEN_METEO_URL, params=params)
    response.raise_for_status()

    weather_data = response.json()

    weather_data["destination"] = {
        "name": name,
        "latitude": latitude,
        "longitude": longitude,
    }

    json_data = json.dumps(weather_data)

    file_client = file_system_client.get_file_client(f"raw/{name}.json")

    file_client.create_file()
    file_client.append_data(
        json_data,  # type: ignore
        offset=0,
        length=len(json_data.encode("utf-8")),
    )
    file_client.flush_data(len(json_data.encode("utf-8")))

    print(f"Uploaded {name}.json")
