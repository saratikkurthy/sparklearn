package com.spark.learn
import org.apache.spark.sql.{SaveMode, SparkSession}


object SQLServerData extends App{
  System.setProperty("hadoop.home.dir", "D:\\hadoop2.6.1")
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Snowflake Connector")
    .getOrCreate();
  val database="SaratLearn";
  val user="Admin";
  val password="xxxxx";
  try {
    //val jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format("SARATSERVER", "1433", "SaratLearn", "SARATSERVER\\Admin", "MyPc@7681")
    val jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format("SARATSERVER", "1433", "SaratLearn")
   /** val df = spark.read
      .format("com.microsoft.sqlserver.jdbc.spark")
      .option("url", "jdbc:sqlserver://{SARATSERVER};databaseName=SaratLearn;")
      .option("dbtable", "EMPLOYEE")
      .option("user", "SARATSERVER\\Admin")
      .option("password", "xxxxx")
      .option("authentication", "ActiveDirectoryPassword")
      .option("hostNameInCertificate", "*.database.windows.net")
      .option("encrypt", "true")
      .load()*/
   val jdbcDF = (spark.read.format("jdbc")
     .option("url",  "jdbc:sqlserver://SARATSERVER:1433;databaseName="+database)
     .option("dbtable", "EMPLOYEE")
     .option("user", user)
     .option("password", password)
     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
     .option("authentication", "ActiveDirectoryPassword")
     .option("hostNameInCertificate", "*.database.windows.net")
     .option("encrypt", "true")
     .option("trustServerCertificate","true")
     .load())


    jdbcDF.show()
  }
  catch {
    case e=> println("Error in fetching details from MS Sql server"+e.printStackTrace())
  }

}
