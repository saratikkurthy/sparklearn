package com.spark.learn
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

object SparkBasics extends App
{
  System.setProperty("hadoop.home.dir", "D:\\hadoop2.6.1")
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SaratLearn")
    .getOrCreate();
  val df = spark.read.option("header", true)
    .csv("D:\\SparkLearningData\\CSV\\Zipcodes.CSV")

  val sqlDF=df.createOrReplaceTempView("Zipcodes")
  df.select("country", "city", "zipcode", "state")
    .show()

  spark.sql("SELECT count(zipcode), city FROM ZIPCODES  group by city")
    .show()
  df.printSchema()
  df.show()
}
