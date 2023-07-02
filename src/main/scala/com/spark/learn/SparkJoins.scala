package com.spark.learn
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
object SparkJoins extends App{
  System.setProperty("hadoop.home.dir", "D:\\hadoop2.6.1")
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkJoins")
    .getOrCreate();
  val employeesDF = spark.read.option("header", true)
    .csv("D:\\SparkLearningData\\CSV\\Employee.CSV")
  val empSQLDF=employeesDF.createOrReplaceTempView("Employees")
  val departmentsDF = spark.read.option("header", true)
    .csv("D:\\SparkLearningData\\CSV\\Department.CSV")
  val deptSQLDF = departmentsDF.createOrReplaceTempView("Departments")
  spark.sql("SELECT e.emp_id,e.name from employees e inner join departments d on e.emp_dept_id=d.dept_id")
    .show(5)
}
