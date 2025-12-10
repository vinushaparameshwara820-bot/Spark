"""
Weather Analysis with PySpark

This script analyzes daily temperature data using PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Weather Analysis") \
        .master("local[*]") \
        .getOrCreate()
    
    print("Spark session created successfully!")
    
    # Load weather.csv
    weather_df = load_weather_data(spark)
    
    # Find hottest day, coldest day
    find_hottest_and_coldest_day(weather_df)
    
    # Average temperature per month
    calculate_average_temperature_per_month(weather_df)
    
    # Count rainy days
    count_rainy_days(weather_df)
    
    # Stop Spark session
    spark.stop()

def load_weather_data(spark):
    """Load weather data from CSV file"""
    print("\n--- Loading weather.csv ---")
    
    # Define schema for the weather data
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("precipitation", DoubleType(), True)
    ])
    
    # Load CSV file
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv("weather.csv")
    
    # Convert date string to date type and extract month
    df_with_date = df.withColumn("parsed_date", to_date(col("date"), "yyyy-MM-dd")) \
        .withColumn("month", month(col("parsed_date"))) \
        .withColumn("year", year(col("parsed_date")))
    
    df_with_date.show()
    return df_with_date

def find_hottest_and_coldest_day(df):
    """Find the hottest and coldest days"""
    print("\n--- Finding Hottest and Coldest Days ---")
    
    # Find the hottest day
    hottest_day = df.orderBy(desc("temperature")).select("date", "temperature").first()
    print(f"Hottest day: {hottest_day['date']} with temperature {hottest_day['temperature']}°F")
    
    # Find the coldest day
    coldest_day = df.orderBy(asc("temperature")).select("date", "temperature").first()
    print(f"Coldest day: {coldest_day['date']} with temperature {coldest_day['temperature']}°F")

def calculate_average_temperature_per_month(df):
    """Calculate average temperature per month"""
    print("\n--- Average Temperature Per Month ---")
    
    # Calculate average temperature per month
    avg_temp_per_month = df.groupBy("month") \
        .agg(round(avg("temperature"), 2).alias("avg_temperature")) \
        .orderBy("month")
    
    avg_temp_per_month.show()

def count_rainy_days(df):
    """Count and display rainy days"""
    print("\n--- Counting Rainy Days ---")
    
    # Count days with precipitation > 0
    rainy_days_count = df.filter(col("precipitation") > 0).count()
    print(f"Number of rainy days: {rainy_days_count}")
    
    # Show rainy days
    print("Rainy days details:")
    df.filter(col("precipitation") > 0) \
        .select("date", "precipitation") \
        .orderBy(desc("precipitation")) \
        .show()

if __name__ == "__main__":
    main()