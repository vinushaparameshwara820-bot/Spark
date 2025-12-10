"""
Simple Weather Analysis in Python

This script analyzes daily temperature data without using Spark.
"""

import csv
from datetime import datetime
from collections import defaultdict
from config import ANALYSIS_YEAR

def load_weather_data(filename, year=ANALYSIS_YEAR):
    """Load weather data from CSV file and filter by year"""
    print(f"--- Loading weather.csv for year {year} ---")
    
    data = []
    with open(filename, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Convert data types
            row['temperature'] = float(row['temperature'])
            row['precipitation'] = float(row['precipitation'])
            row['date'] = datetime.strptime(row['date'], '%Y-%m-%d')
            
            # Filter data by year
            if row['date'].year == year:
                data.append(row)
    
    if not data:
        print(f"No data found for year {year}. Available data may be for a different year.")
        return []
            
    # Display loaded data
    print(f"{'Date':<12} {'Temperature':<12} {'Precipitation':<12}")
    print("-" * 36)
    for row in data[:10]:  # Show first 10 rows
        print(f"{row['date'].strftime('%Y-%m-%d'):<12} {row['temperature']:<12} {row['precipitation']:<12}")
    if len(data) > 10:
        print("...")
    
    return data

def find_hottest_and_coldest_day(data):
    """Find the hottest and coldest days"""
    if not data:
        return
        
    print("\n--- Finding Hottest and Coldest Days ---")
    
    # Find the hottest day
    hottest_day = max(data, key=lambda x: x['temperature'])
    print(f"Hottest day: {hottest_day['date'].strftime('%Y-%m-%d')} with temperature {hottest_day['temperature']}°F")
    
    # Find the coldest day
    coldest_day = min(data, key=lambda x: x['temperature'])
    print(f"Coldest day: {coldest_day['date'].strftime('%Y-%m-%d')} with temperature {coldest_day['temperature']}°F")

def calculate_average_temperature_per_month(data):
    """Calculate average temperature per month"""
    if not data:
        return
        
    print("\n--- Average Temperature Per Month ---")
    
    # Group temperatures by month
    monthly_temps = defaultdict(list)
    for row in data:
        month_key = row['date'].strftime('%Y-%m')
        monthly_temps[month_key].append(row['temperature'])
    
    # Calculate averages
    print(f"{'Month':<10} {'Avg Temp':<10}")
    print("-" * 20)
    for month, temps in sorted(monthly_temps.items()):
        avg_temp = sum(temps) / len(temps)
        print(f"{month:<10} {avg_temp:<10.2f}")

def count_rainy_days(data):
    """Count and display rainy days"""
    if not data:
        return
        
    print("\n--- Counting Rainy Days ---")
    
    # Count days with precipitation > 0
    rainy_days = [row for row in data if row['precipitation'] > 0]
    rainy_days_count = len(rainy_days)
    print(f"Number of rainy days: {rainy_days_count}")
    
    # Show rainy days sorted by precipitation (highest first)
    print("\nRainy days details (top 5):")
    print(f"{'Date':<12} {'Precipitation':<12}")
    print("-" * 24)
    for row in sorted(rainy_days, key=lambda x: x['precipitation'], reverse=True)[:5]:
        print(f"{row['date'].strftime('%Y-%m-%d'):<12} {row['precipitation']:<12}")

def main():
    try:
        # Load weather.csv
        weather_data = load_weather_data('weather.csv')
        
        # Find hottest day, coldest day
        find_hottest_and_coldest_day(weather_data)
        
        # Average temperature per month
        calculate_average_temperature_per_month(weather_data)
        
        # Count rainy days
        count_rainy_days(weather_data)
        
    except FileNotFoundError:
        print("Error: weather.csv file not found. Please make sure the file exists in the current directory.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()