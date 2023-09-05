package com.spark.dao
import org.apache.spark.sql.{SaveMode, SparkSession}
object MySQLDataAccess extends App{
  System.setProperty("hadoop.home.dir", "D:\\hadoop2.6.1")

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("MYSQL Read Example")
    .getOrCreate();
  val df = spark.read
    .format("jdbc")
    .option("driver","com.mysql.cj.jdbc.Driver")
    .option("url", "jdbc:mysql://localhost:3306/saratlearn")
    .option("dbtable", "employee")
    .option("user", "root")
    .option("password", "")
    .load()
  //df.show()

  var sfOptions = Map(
    "sfURL" -> "https://aflmryh-dt23632.snowflakecomputing.com/",
    "sfAccount" -> "aflmryh-dt23632",
    "sfUser" -> "SARATIKKURTHY",
    "sfPassword" -> "",
    "sfDatabase" -> "SARATLEARN",
    "sfSchema" -> "TEST",
    "sfRole" -> "SYSADMIN"
  )
  import spark.implicits._
  for( i <- 1 to 1000) {
    df.write
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions)
      .option("dbtable", "EMPLOYEE")
      .mode(SaveMode.Append)
      .save()
  }
}
