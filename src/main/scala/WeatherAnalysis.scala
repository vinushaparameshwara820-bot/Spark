import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object WeatherAnalysis {
  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("Weather Analysis")
      .master("local[*]")
      .getOrCreate()
    
    import spark.implicits._
    
    // Load weather.csv
    val weatherDF = loadWeatherData(spark)
    
    // Find hottest day, coldest day
    findHottestAndColdestDay(weatherDF)
    
    // Average temperature per month
    calculateAverageTemperaturePerMonth(weatherDF)
    
    // Count rainy days
    countRainyDays(weatherDF)
    
    spark.stop()
  }
  
  def loadWeatherData(spark: SparkSession): DataFrame = {
    println("Loading weather.csv...")
    
    // Define schema for the weather data
    val schema = StructType(Array(
      StructField("date", StringType, true),
      StructField("temperature", DoubleType, true),
      StructField("precipitation", DoubleType, true)
    ))
    
    // Load CSV file
    val df = spark.read
      .option("header", "true")
      .schema(schema)
      .csv("weather.csv")
    
    // Convert date string to date type and extract month
    val dfWithDate = df.withColumn("parsed_date", to_date(col("date"), "yyyy-MM-dd"))
      .withColumn("month", month(col("parsed_date")))
      .withColumn("year", year(col("parsed_date")))
    
    dfWithDate.show()
    dfWithDate
  }
  
  def findHottestAndColdestDay(df: DataFrame): Unit = {
    println("\n--- Finding Hottest and Coldest Days ---")
    
    // Find the hottest day
    val hottestDay = df.orderBy(desc("temperature")).select("date", "temperature").first()
    println(s"Hottest day: ${hottestDay.getAs[String]("date")} with temperature ${hottestDay.getAs[Double]("temperature")}°F")
    
    // Find the coldest day
    val coldestDay = df.orderBy(asc("temperature")).select("date", "temperature").first()
    println(s"Coldest day: ${coldestDay.getAs[String]("date")} with temperature ${coldestDay.getAs[Double]("temperature")}°F")
  }
  
  def calculateAverageTemperaturePerMonth(df: DataFrame): Unit = {
    println("\n--- Average Temperature Per Month ---")
    
    // Calculate average temperature per month
    val avgTempPerMonth = df.groupBy("month")
      .agg(round(avg("temperature"), 2).as("avg_temperature"))
      .orderBy("month")
    
    avgTempPerMonth.show()
  }
  
  def countRainyDays(df: DataFrame): Unit = {
    println("\n--- Counting Rainy Days ---")
    
    // Count days with precipitation > 0
    val rainyDaysCount = df.filter(col("precipitation") > 0).count()
    println(s"Number of rainy days: $rainyDaysCount")
    
    // Show rainy days
    println("Rainy days details:")
    df.filter(col("precipitation") > 0)
      .select("date", "precipitation")
      .orderBy(desc("precipitation"))
      .show()
  }
}