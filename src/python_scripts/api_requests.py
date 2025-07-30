import requests
from dotenv import load_dotenv
import os
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path)

print(project_root)
print(env_path)

API_KEY = os.getenv("API_KEY")
api_url = f"http://api.weatherstack.com/current?access_key={API_KEY}&query=New York"

def fetch_data():
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        print("API response received successfully!")
        return data

    except requests.exceptions.RequestException as e:
        print(f"An error occurred when the request was made: {e}")
        raise

def mock_fetch_data():
    return {'request': {'type': 'City', 'query': 'New York, United States of America', 'language': 'en', 'unit': 'm'}, 'location': {'name': 'New York', 'country': 'United States of America', 'region': 'New York', 'lat': '40.714', 'lon': '-74.006', 'timezone_id': 'America/New_York', 'localtime': '2025-07-28 02:14', 'localtime_epoch': 1753668840, 'utc_offset': '-4.0'}, 'current': {'observation_time': '06:14 AM', 'temperature': 27, 'weather_code': 113, 'weather_icons': ['https://cdn.worldweatheronline.com/images/wsymbols01_png_64/wsymbol_0008_clear_sky_night.png'], 'weather_descriptions': ['Clear '], 'astro': {'sunrise': '05:49 AM', 'sunset': '08:15 PM', 'moonrise': '09:50 AM', 'moonset': '10:19 PM', 'moon_phase': 'Waxing Crescent', 'moon_illumination': 11}, 'air_quality': {'co': '464.35', 'no2': '27.75', 'o3': '60', 'so2': '7.215', 'pm2_5': '12.95', 'pm10': '13.32', 'us-epa-index': '1', 'gb-defra-index': '1'}, 'wind_speed': 10, 'wind_degree': 301, 'wind_dir': 'WNW', 'pressure': 1015, 'precip': 0, 'humidity': 76, 'cloudcover': 0, 'feelslike': 30, 'uv_index': 0, 'visibility': 16, 'is_day': 'no'}}