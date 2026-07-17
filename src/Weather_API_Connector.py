import requests
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine
import json

# REFACTOR: Move to environment/config
HEADERS = {
    "User-Agent": "weather-pipeline/0.1 (bloodshard614@gmail.com)",
    "Accept": "application/geo+json"
}


def nws_get(url: str) -> dict:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def latest_station_observation(station_id: str) -> dict:
    url = f"https://api.weather.gov/stations/{station_id}/observations/latest"
    data = nws_get(url)
    props = data["properties"]
    print(url)

    return {
        "station_id": station_id,
        "station_name": props.get("stationName"),
        "observed_at": props.get("timestamp"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),

        "latitude": data.get("geometry", {}).get("coordinates", [None, None])[1],
        "longitude": data.get("geometry", {}).get("coordinates", [None, None])[0],
        "elevation_m": props.get("elevation", {}).get("value"),

        "temperature_c": props.get("temperature", {}).get("value"),
        "dewpoint_c": props.get("dewpoint", {}).get("value"),
        "relative_humidity_pct": props.get("relativeHumidity", {}).get("value"),

        "wind_direction_deg": props.get("windDirection", {}).get("value"),
        "wind_speed_kmh": props.get("windSpeed", {}).get("value"),
        "wind_gust_kmh": props.get("windGust", {}).get("value"),

        "barometric_pressure_pa": props.get("barometricPressure", {}).get("value"),
        "sea_level_pressure_pa": props.get("seaLevelPressure", {}).get("value"),
        "visibility_m": props.get("visibility", {}).get("value"),

        "heat_index_c": props.get("heatIndex", {}).get("value"),
        "wind_chill_c": props.get("windChill", {}).get("value"),
        "precipitation_last_3h_mm": props.get("precipitationLast3Hours", {}).get("value"),

        "text_description": props.get("textDescription"),
        "raw_message": props.get("rawMessage"),
        "cloud_layers": props.get("cloudLayers"),
        "present_weather": props.get("presentWeather"),

        "raw": data,
    }


# REFACTOR: Move to environment/config
STATIONS = [
    "KATL", "KLAX", "KORD", "KDFW", "KDEN", "KJFK", "KSFO", "KSEA",
    "KMCO", "KMIA", "KBOS", "KEWR", "KIAD", "KDCA", "KCLT", "KPHX",
    "KIAH", "KMSP", "KDTW", "KPHL", "KSLC", "KTPA", "KSAN", "KLAS",
    "KPDX", "KRDU", "KBNA", "KAUS", "KBDL", "KCLE", "KCVG", "KPIT",
    "KSTL", "KMCI", "KIND", "KSMF", "KMSY", "PANC", "PHNL", "TJSJ"
]


def build_weather_dataframe():
    weather_update = pd.DataFrame(
        [latest_station_observation(station) for station in STATIONS]
    )

    json_cols = ["raw", "cloud_layers", "present_weather"]

    for col in json_cols:
        weather_update[col] = weather_update[col].apply(
            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
        )

    return weather_update


def database_connection():
    engine = create_engine(
        "postgresql+psycopg2://postgres:password@localhost:5432/postgres"
    )

    return engine


def filter_new_records(weather_update, engine):

    existing_records = pd.read_sql(
        """
        SELECT station_id, observed_at
        FROM public.weather_update
        """,
        con=engine
    )

    weather_update["observed_at"] = pd.to_datetime(
        weather_update["observed_at"],
        utc=True
    )

    existing_records["observed_at"] = pd.to_datetime(
        existing_records["observed_at"],
        utc=True
    )

    weather_update = weather_update.merge(
        existing_records,
        on=["station_id", "observed_at"],
        how="left",
        indicator=True
    )

    weather_update = weather_update[
        weather_update["_merge"] == "left_only"
    ]

    return weather_update.drop(columns=["_merge"])


def insert_new_records(weather_update, engine):

    if len(weather_update) > 0:
        weather_update.to_sql(
            name="weather_update",
            con=engine,
            schema="public",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )
        print(f"Inserted rows: {len(weather_update)}")
    else:
        print("No new records to insert")


def main():
    engine = database_connection()

    weather_update = build_weather_dataframe()

    weather_update = filter_new_records(
        weather_update,
        engine
    )

    insert_new_records(
        weather_update,
        engine
    )


if __name__ == "__main__":
    main()