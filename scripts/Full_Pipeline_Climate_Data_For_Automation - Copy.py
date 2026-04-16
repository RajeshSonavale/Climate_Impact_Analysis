import os 
# Change working directory to script location
os.chdir(os.path.dirname(os.path.abspath(__file__)))

import datetime
with open("log.txt", "a") as f:
    f.write(f"{datetime.datetime.now()}: Script started\n")

import pandas as pd
import requests
import os
from datetime import datetime
import numpy as np

# -------------------------------
# 1️⃣ CONFIG
# -------------------------------
API_KEY = "2c756e7f9e35107c881ab434f7cf06d7"  
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

CITIES_FILE = "../data/raw/cities_master.csv"                  # 100 cities list
RAW_FILE = "../data/raw/raw_weather_data.csv"                   # hourly raw data
CLEANED_FILE = "../data/processed/clean_weather_data.csv"    # cleaned data
FINAL_FILE = "../data/processed/climate_dashboard_dataset.csv" # aggregated for dashboard

# Ensure folders exist
os.makedirs("../data/raw", exist_ok=True)
os.makedirs("../data/processed", exist_ok=True)
os.makedirs("../data/final", exist_ok=True)


# -------------------------------
# 2️⃣ Fetch Data
# -------------------------------
def fetch_data():
    print("Fetching data from OpenWeather API...")

    if not os.path.exists(CITIES_FILE):
        print(f"ERROR: Cities file not found: {CITIES_FILE}")
        return

    cities_df = pd.read_csv(CITIES_FILE)
    data_list = []

    for city in cities_df["City"]:
        params = {"q": city, "appid": API_KEY, "units": "metric"}
        try:
            response = requests.get(BASE_URL, params=params).json()
            if response.get("main"):
                data_list.append({
                    "City": city,
                    "Temperature": response["main"]["temp"],
                    "Humidity": response["main"]["humidity"],
                    "Pressure": response["main"]["pressure"],
                    "Datetime": datetime.now()
                })
        except Exception as e:
            print(f"API error for {city}: {e}")

    if not data_list:
        print("No data fetched from API.")
        return

    weather_df = pd.DataFrame(data_list)

    # Save raw data with proper datetime format
    if not os.path.exists(RAW_FILE):
        weather_df.to_csv(RAW_FILE, mode="w", header=True, index=False,
                          date_format="%Y-%m-%d %H:%M:%S")
    else:
        weather_df.to_csv(RAW_FILE, mode="a", header=False, index=False,
                          date_format="%Y-%m-%d %H:%M:%S")

    print(f"Raw data saved: {RAW_FILE}")


# -------------------------------
# 3️⃣ Clean Data
# -------------------------------
def clean_data():
    print("Cleaning data...")

    if os.path.exists(CLEANED_FILE):
        df_old = pd.read_csv(CLEANED_FILE, parse_dates=["Datetime"])
    else:
        df_old = pd.DataFrame(columns=["City", "Temperature", "Humidity", "Pressure", "Datetime"])

    if not os.path.exists(RAW_FILE):
        print(f"ERROR: Raw data file not found: {RAW_FILE}")
        return

    df_new = pd.read_csv(RAW_FILE, parse_dates=["Datetime"])
    df = pd.concat([df_old, df_new], ignore_index=True)

    # Convert numeric columns safely to handle ValueError
    for col in ["Temperature", "Humidity", "Pressure"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")  # non-numeric → NaN

    # Fill missing values
    df["Temperature"].fillna(df["Temperature"].mean(), inplace=True)
    df["Humidity"].fillna(df["Humidity"].median(), inplace=True)
    df["Pressure"].fillna(df["Pressure"].median(), inplace=True)

    # Outlier capping (IQR method)
    for col in ["Temperature", "Humidity", "Pressure"]:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        df[col] = df[col].clip(lower, upper)

    
    # Remove duplicates (City + Datetime)
    df.drop_duplicates(subset=["City", "Datetime"], keep="last", inplace=True)

    # Save cleaned data
    df.to_csv(CLEANED_FILE, index=False, date_format="%Y-%m-%d %H:%M:%S")
    print(f"Cleaned data saved: {CLEANED_FILE}")


# -------------------------------
# 4️⃣ Aggregate for Dashboard
# -------------------------------
def aggregate_data():
    print("Aggregating data for dashboard...")

    if not os.path.exists(CLEANED_FILE):
        print(f"ERROR: Cleaned data file not found: {CLEANED_FILE}")
        return

    df = pd.read_csv(CLEANED_FILE, parse_dates=["Datetime"])
    df["Date"] = df["Datetime"].dt.date  # only date for daily aggregation

    agg_df = df.groupby(["City", "Date"]).agg({
        "Temperature": "max",  # daily max temp
        "Humidity": "mean",    # daily mean humidity
        "Pressure": "mean"     # daily mean pressure
    }).reset_index()

    agg_df.to_csv(FINAL_FILE, index=False)
    print(f"Aggregated data saved: {FINAL_FILE}")


# -------------------------------
# 5️⃣ Run Full Pipeline
# -------------------------------
def run_pipeline():
    fetch_data()
    clean_data()
    aggregate_data()
    print("✅ Full pipeline executed successfully!")


# -------------------------------
# 6️⃣ Execute
# -------------------------------
if __name__ == "__main__":
    run_pipeline()


import datetime
with open("log.txt", "a") as f:
    f.write(f"{datetime.datetime.now()}: Script completed\n\n")


