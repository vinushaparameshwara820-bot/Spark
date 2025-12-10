#!/usr/bin/env python3
"""
Script to change the year in weather.csv data
"""

import csv
from datetime import datetime
import os

def change_year_in_csv(input_file, output_file, new_year):
    """Change the year in the weather CSV file"""
    
    # Read the existing data
    data = []
    with open(input_file, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)  # Read header
        data.append(header)
        
        for row in reader:
            # Parse the date (first column)
            date_str = row[0]
            # Extract month and day, replace year
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            new_date = date_obj.replace(year=new_year)
            new_date_str = new_date.strftime('%Y-%m-%d')
            
            # Update the row with new date
            new_row = [new_date_str] + row[1:]
            data.append(new_row)
    
    # Write the updated data
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)
    
    return len(data) - 1  # Return number of data rows (excluding header)

def main():
    print("🌤️ Weather Data Year Changer")
    print("=" * 30)
    
    # Check if weather.csv exists
    if not os.path.exists('weather.csv'):
        print("❌ Error: weather.csv file not found!")
        return
    
    try:
        # Get the new year from user
        new_year_input = input("Enter the new year (e.g., 2025): ")
        new_year = int(new_year_input)
        
        if new_year < 1900 or new_year > 2100:
            print("❌ Error: Please enter a reasonable year between 1900 and 2100")
            return
        
        # Change the year in the CSV file
        rows_updated = change_year_in_csv('weather.csv', 'weather.csv', new_year)
        
        print(f"✅ Successfully updated {rows_updated} records to year {new_year}")
        print(f"💾 Changes saved to weather.csv")
        
        # Show a sample of the updated data
        print("\n📋 Sample of updated data:")
        with open('weather.csv', 'r') as file:
            for i, line in enumerate(file):
                if i <= 5:  # Show first 5 lines
                    print(line.strip())
                else:
                    print("...")
                    break
        
    except ValueError:
        print("❌ Error: Please enter a valid year (numbers only)")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()