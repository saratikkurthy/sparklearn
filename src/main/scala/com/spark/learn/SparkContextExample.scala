package com.spark.learn


import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

object SparkContextExample extends App{
  System.setProperty("hadoop.home.dir", "D:\\hadoop2.6.1")
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SaratLearn")
    .getOrCreate();

  spark.sparkContext.setLogLevel("ERROR")


  val sparkContext:SparkContext = spark.sparkContext


  println("First SparkContext:")
  println("APP Name :"+spark.sparkContext.appName);
  println("Deploy Mode :"+spark.sparkContext.deployMode);
  println("Master :"+spark.sparkContext.master);

  val sparkSession2 = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample-test")
    .getOrCreate();

  println("Second SparkContext:")
  println("APP Name :"+sparkSession2.sparkContext.appName);
  println("Deploy Mode :"+sparkSession2.sparkContext.deployMode);
  println("Master :"+sparkSession2.sparkContext.master);


}