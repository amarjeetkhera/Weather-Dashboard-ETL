"""
Project: Live Weather Intelligence Dashboard
Author: Amarjeet Khera
Description: Python ETL pipeline fetching real-time data from OpenMeteo API.
             Includes caching, error handling, and geospatial timezone alignment.
Use Case: Environmental data acquisition for Maritime Bio-Fouling prediction.
"""

import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from timezonefinder import TimezoneFinder

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Required weather parameters (daily and hourly)
url = "https://api.open-meteo.com/v1/forecast"
location = {"Washington":[38.9072, -77.0369], "Bogota":[4.6097, -74.0817],
            "London":[51.5085,-0.1257], "Berlin":[52.5244, 13.4105],
            "Paris":[48.8566, 2.3522], "Madrid":[40.4168, -3.7038],
            "Tokyo":[35.6895, 139.6917], "Beijing":[39.9042, 116.4074],
			"Moscow":[55.7558, 37.6173], "Cairo":[30.0444, 31.2357],
			"Mexico":[19.4326, -99.1332], "Rio de Janeiro":[-22.9068, -43.1729],
            "Mumbai":[19.0760, 72.8777], "Sydney":[-33.8688, 151.2093]}

daily_data_2 = pd.DataFrame(columns=["Location", "Date", "Max Wind speed", "Dominant Wind direction", "Weather Code"])
hourly_data = pd.DataFrame(columns=["Location", "Weather Code", "Wind Speed"])
tf = TimezoneFinder() #Initializing the timezone finder

for i in location.keys():
	params = {
		"latitude": location[i][0],
		"longitude": location[i][1],
		"daily": ["wind_speed_10m_max", "wind_direction_10m_dominant", "weather_code"],
		"hourly": ["temperature_2m", "weather_code", "wind_speed_10m"],
		"forecast_days": 7
	}
	responses = openmeteo.weather_api(url, params=params)

	# Process first location
	response = responses[0]

	# Process daily data. (The order of variables needs to be the same as requested)
	daily = response.Daily()
	daily_weather_code = daily.Variables(2).ValuesAsNumpy()
	daily_wind_speed_10m_max = daily.Variables(0).ValuesAsNumpy()
	daily_wind_direction_10m_dominant = daily.Variables(1).ValuesAsNumpy()

	daily_data_2 = pd.concat([daily_data_2, pd.DataFrame([{
		"Location": i,
		"Date": pd.date_range(
			start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
			end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
			freq = pd.Timedelta(seconds = daily.Interval()),
			inclusive = "left"),
		"Max Wind speed": daily_wind_speed_10m_max,
		"Dominant Wind direction": daily_wind_direction_10m_dominant,
		"Weather Code": daily_weather_code
	}])], ignore_index = True)
	daily_data_2 = daily_data_2.explode(["Date", "Max Wind speed", "Dominant Wind direction", "Weather Code"])

  # Process hourly data. (The order of variables needs to be the same as requested)
	hourly = response.Hourly()
	hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
	hourly_weather_code = hourly.Variables(1).ValuesAsNumpy()
	hourly_wind_speed_10m = hourly.Variables(2).ValuesAsNumpy()

	hourly_data = pd.concat([hourly_data, pd.DataFrame([{
		"Location": i,
		"Date": pd.date_range(
			start = pd.to_datetime(hourly.Time(), unit = "s", utc = True).tz_convert(tf.timezone_at(lat = location[i][0], lng = location[i][1])),
			end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True).tz_convert(tf.timezone_at(lat = location[i][0], lng = location[i][1])),
			freq = pd.Timedelta(seconds = hourly.Interval()),
			inclusive = "left"),
		"Temperature": hourly_temperature_2m,
		"Weather Code": hourly_weather_code,
		"Wind Speed": hourly_wind_speed_10m
	}])], ignore_index = True)

	hourly_data = hourly_data.explode(["Date", "Temperature", "Weather Code", "Wind Speed"])
