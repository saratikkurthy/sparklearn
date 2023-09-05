package com.spark.snowflake
import org.apache.spark.sql.{SaveMode, SparkSession}

object SnowflakeConnect extends App{
  System.setProperty("hadoop.home.dir", "D:\\hadoop2.6.1")
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Snowflake Connector")
    //.config("spark.jars", "jar/snowflake-jdbc:3.13.30:jar,jar/spark-snowflake_2.12-2.9.0-spark_2.4.jar")
    .getOrCreate();

  import spark.implicits._

  val simpleData = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("Raman", "Finance", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Varshini", "Ikkurthy", 520741)
  )
  val df = simpleData.toDF("name", "department", "salary")
  df.show()
  //https://cv63407.europe-west4.gcp.snowflakecomputing.com
  var sfOptions = Map(
    "sfURL" -> "https://aflmryh-dt23632.snowflakecomputing.com/",
    "sfAccount" -> "aflmryh-dt23632",
    "sfUser" -> "SARATIKKURTHY",
    "sfPassword" -> "",
    "sfDatabase" -> "SARATLEARN",
    "sfSchema" -> "TEST",
    "sfRole" -> "SYSADMIN"
  )

  try {
    df.write
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions)
      .option("dbtable", "EMPLOYEE")
      .mode(SaveMode.Overwrite)
      .save()
  }
  catch {
    case e =>println("Excepting while saving data in snowflake"+e.printStackTrace())
  }
}
