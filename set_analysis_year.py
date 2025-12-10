#!/usr/bin/env python3
"""
Script to set the analysis year in config.py
"""

import os
import re

def update_config_year(new_year):
    """Update the ANALYSIS_YEAR in config.py"""
    
    config_file = 'config.py'
    
    # Check if config.py exists
    if not os.path.exists(config_file):
        print("❌ Error: config.py file not found!")
        return False
    
    try:
        # Read the current config file
        with open(config_file, 'r') as file:
            content = file.read()
        
        # Update the ANALYSIS_YEAR line
        updated_content = re.sub(
            r'ANALYSIS_YEAR\s*=\s*\d+', 
            f'ANALYSIS_YEAR = {new_year}', 
            content
        )
        
        # Also update the DEFAULT_YEAR line to match
        updated_content = re.sub(
            r'DEFAULT_YEAR\s*=\s*\d+', 
            f'DEFAULT_YEAR = {new_year}', 
            updated_content
        )
        
        # Write the updated content back to the file
        with open(config_file, 'w') as file:
            file.write(updated_content)
        
        return True
        
    except Exception as e:
        print(f"❌ Error updating config.py: {e}")
        return False

def main():
    print("📅 Set Weather Analysis Year")
    print("=" * 30)
    
    try:
        # Get the new year from user
        new_year_input = input("Enter the year to analyze (e.g., 2025): ")
        new_year = int(new_year_input)
        
        if new_year < 1900 or new_year > 2100:
            print("❌ Error: Please enter a reasonable year between 1900 and 2100")
            return
        
        # Update the configuration
        if update_config_year(new_year):
            print(f"✅ Successfully set analysis year to {new_year}")
            print("💾 Changes saved to config.py")
            print(f"\n🔄 All analysis tools will now process data for {new_year}")
        else:
            print("❌ Failed to update the configuration")
        
    except ValueError:
        print("❌ Error: Please enter a valid year (numbers only)")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()