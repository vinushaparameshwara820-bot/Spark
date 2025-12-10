#!/bin/bash

echo "Starting Weather Analysis with Apache Spark..."

# Check if sbt is installed
if ! command -v sbt &> /dev/null
then
    echo "SBT is not installed. Please install SBT to run this project."
    exit 1
fi

# Run the Spark application
sbt run

echo "Weather Analysis completed."