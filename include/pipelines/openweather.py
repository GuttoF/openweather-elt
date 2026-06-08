import os
from http.client import REQUEST_TIMEOUT
from typing import Iterator

import dlt
import requests
from capitals import CAPITALS

BASE = "https://api.openweathermap.org/data/2.5"

try:
    # Caso seja run local
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def _api_key() -> str:
    key = os.environ.get("API_KEY")
    if not key:
        raise RuntimeError("Insira a API_KEY com sua chave da OpenWeather.")
    return key


def _dw_credentials() -> str:
    host = os.environ.get("DW_HOST", "localhost")
    port = os.environ.get("DW_PORT", "5433")
    user = os.environ.get("DW_USER", "weather")
    password = os.environ.get("DW_PASSWORD", "weather")
    db = os.environ.get("DW_DB", "weather")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


@dlt.resource(name="current_weather", write_disposition="append")
def current_weather() -> Iterator[dict]:
    key = _api_key()
    for cap in CAPITALS:
        resp = requests.get(
            f"{BASE}/weather",
            params={"lat": cap["lat"], "lon": cap["lon"], "appid": key},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        weather = (data.get("weather") or [{}])[0]
        main = data.get("main", {})
        wind = data.get("wind", {})
        yield {
            "city": cap["name"],
            "uf": cap["uf"],
            "lat": cap["lat"],
            "lon": cap["lon"],
            "country": data.get("sys", {}).get("country"),
            "observed_at_unix": data.get("dt"),
            "weather_main": weather.get("main"),
            "weather_description": weather.get("description"),
            "temp_k": main.get("temp"),
            "feels_like_k": main.get("feels_like"),
            "temp_min_k": main.get("temp_min"),
            "temp_max_k": main.get("temp_max"),
            "pressure": main.get("pressure"),
            "humidity": main.get("humidity"),
            "wind_speed": wind.get("speed"),
            "wind_deg": wind.get("deg"),
            "clouds_all": data.get("clouds", {}).get("all"),
        }


@dlt.resource(name="forecast", write_disposition="append")
def forecast() -> Iterator[dict]:
    key = _api_key()
    for cap in CAPITALS:
        resp = requests.get(
            f"{BASE}/forecast",
            params={"lat": cap["lat"], "lon": cap["lon"], "appid": key},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        for point in data.get("list", []):
            weather = (point.get("weather") or [{}])[0]
            main = point.get("main", {})
            wind = point.get("wind", {})
            yield {
                "city": cap["name"],
                "uf": cap["uf"],
                "lat": cap["lat"],
                "lon": cap["lon"],
                "country": data.get("city", {}).get("country"),
                "forecast_at_unix": point.get("dt"),
                "forecast_at_txt": point.get("dt_txt"),
                "weather_main": weather.get("main"),
                "weather_description": weather.get("description"),
                "temp_k": main.get("temp"),
                "feels_like_k": main.get("feels_like"),
                "temp_min_k": main.get("temp_min"),
                "temp_max_k": main.get("temp_max"),
                "pressure": main.get("pressure"),
                "humidity": main.get("humidity"),
                "wind_speed": wind.get("speed"),
                "wind_deg": wind.get("deg"),
                "clouds_all": point.get("clouds", {}).get("all"),
                "pop": point.get("pop"),
            }


@dlt.source(name="openweather")
def openweather_source():
    return current_weather(), forecast()


def run() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="openweather",
        destination=dlt.destinations.postgres(credentials=_dw_credentials()),
        dataset_name="raw_openweather",
    )
    load_info = pipeline.run(openweather_source())
    print(load_info)


if __name__ == "__main__":
    run()
