import requests

# User inputs
lat = input("Enter latitude: ")
lon = input("Enter longitude: ")
date = input("Enter date (YYYY-MM-DD): ")

# Open-Meteo API URL for hourly temperature and precipitation
url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={date}&end_date={date}&hourly=temperature_2m,precipitation&timezone=auto"

# API request
response = requests.get(url)

if response.status_code == 200:
    data = response.json()

    # Extract hourly temperatures and precipitation
    temperatures = data["hourly"]["temperature_2m"]
    precipitation = data["hourly"]["precipitation"]

    # Calculate average temperature
    avg_temp = sum(temperatures) / len(temperatures)

    # Calculate total daily precipitation
    total_precipitation = sum(precipitation)

    print(f"Weather Data for {date}:")
    print(f"Average Temperature: {avg_temp:.2f}°C")
    print(f"Total Precipitation: {total_precipitation:.2f} mm")
else:
    print(f"Error fetching data: {response.status_code}")
    print(response.json())  # Print error details
