"""
Web Dashboard for Weather Analysis
"""

from flask import Flask, render_template, jsonify, request
import csv
from datetime import datetime
from collections import defaultdict

app = Flask(__name__)

def load_weather_data(year=None):
    """Load weather data from CSV file and filter by year"""
    data = []
    with open('weather.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Convert data types
            row['temperature'] = float(row['temperature'])
            row['precipitation'] = float(row['precipitation'])
            row['date'] = datetime.strptime(row['date'], '%Y-%m-%d')
            
            # Filter data by year if specified
            if year is None or row['date'].year == int(year):
                data.append(row)
    return data

def analyze_weather_data(data):
    """Analyze weather data and return results"""
    # Handle empty data
    if not data:
        return {
            'hottest_day': {'date': 'N/A', 'temperature': 0},
            'coldest_day': {'date': 'N/A', 'temperature': 0},
            'monthly_averages': {},
            'rainy_days_count': 0,
            'top_rainy_days': []
        }
    
    # Find hottest and coldest days
    hottest_day = max(data, key=lambda x: x['temperature'])
    coldest_day = min(data, key=lambda x: x['temperature'])
    
    # Calculate average temperature per month
    monthly_temps = defaultdict(list)
    for row in data:
        month_key = row['date'].strftime('%Y-%m')
        monthly_temps[month_key].append(row['temperature'])
    
    monthly_averages = {}
    for month, temps in sorted(monthly_temps.items()):
        monthly_averages[month] = round(sum(temps) / len(temps), 2)
    
    # Count rainy days
    rainy_days = [row for row in data if row['precipitation'] > 0]
    rainy_days_count = len(rainy_days)
    
    # Top 5 rainy days
    top_rainy_days = sorted(rainy_days, key=lambda x: x['precipitation'], reverse=True)[:5]
    
    return {
        'hottest_day': {
            'date': hottest_day['date'].strftime('%Y-%m-%d'),
            'temperature': hottest_day['temperature']
        },
        'coldest_day': {
            'date': coldest_day['date'].strftime('%Y-%m-%d'),
            'temperature': coldest_day['temperature']
        },
        'monthly_averages': monthly_averages,
        'rainy_days_count': rainy_days_count,
        'top_rainy_days': [{
            'date': day['date'].strftime('%Y-%m-%d'),
            'precipitation': day['precipitation']
        } for day in top_rainy_days]
    }

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/api/weather-data')
def weather_data():
    """API endpoint for weather data"""
    try:
        # Get year parameter from query string
        year = request.args.get('year', None)
        data = load_weather_data(year)
        analysis = analyze_weather_data(data)
        return jsonify(analysis)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='localhost', port=5000, debug=True)