import requests
from datetime import datetime
import os

def extract_weather(**context):
    api_key = os.getenv("WEATHER_API_KEY")
    city = "Sao Paulo"

    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

    response = requests.get(url)
    data = response.json()

    result = {
        "city": data["name"],
        "temperature": data["main"]["temp"],
        "collected_at": datetime.utcnow().isoformat()
    }

    return result
