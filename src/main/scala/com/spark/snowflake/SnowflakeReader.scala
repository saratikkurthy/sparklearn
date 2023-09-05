package com.spark.snowflake
import org.apache.spark.sql.SparkSession;
object SnowflakeReader extends App{
  System.setProperty("hadoop.home.dir", "D:\\hadoop2.6.1")

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Snowflake Reader")
    .getOrCreate();
  var sfOptions = Map(
    "sfURL" -> "https://aflmryh-dt23632.snowflakecomputing.com/",
    "sfAccount" -> "aflmryh-dt23632",
    "sfUser" -> "SARATIKKURTHY",
    "sfPassword" -> "",
    "sfDatabase" -> "SARATLEARN",
    "sfSchema" -> "TEST",
    "sfRole" -> "SYSADMIN"
  )
  val df = spark.read
    .format("net.snowflake.spark.snowflake")
    .options(sfOptions)
    .option("dbtable", "EMPLOYEE")
    .load()
  df.show()

}
