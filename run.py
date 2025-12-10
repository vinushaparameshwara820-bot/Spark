#!/usr/bin/env python3
"""
Runner script for the weather analysis project
"""

import subprocess
import sys

def main():
    print("Starting Weather Analysis...")
    print("Using simple Python version (no Spark required)")
    
    try:
        # Run the simple weather analysis script
        result = subprocess.run([sys.executable, "simple_weather_analysis.py"], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print("Weather Analysis completed successfully!")
            print(result.stdout)
        else:
            print("Error running Weather Analysis:")
            print(result.stderr)
            
    except FileNotFoundError:
        print("Error: Python not found. Please ensure Python is installed and in your PATH.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()