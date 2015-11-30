package com.hs

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf



object DataMungingCorrObject {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataMungingCorr")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //read the env data
    val env = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("env_data.csv")
    
    //read the machine data
    val machine = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("machine_data.csv")
    /**
     * Since Environment data is collected every minute, convert the data collection time format to minute level
     */
    val m = machine.select(regexp_replace(machine("reading_time"), "^((?:[^:]*\\:){2})[^:]+", "$100"), machine("current"), machine("type")).withColumnRenamed("regexp_replace(reading_time,^((?:[^:]*\\:){2})[^:]+,$100)", "time")
    
    //join the 2 tables on reading time
    val join = env.join(m, env.col("readtime") === m.col("time")).filter(m.col("current") > 0)
    
    //extract the data for each machine type and selecting only needed columns
    val lathe = join.select("current", "temperature", "pressure", "humidity", "type").filter(join("type") === "lathe")
    val mill = join.select("current", "temperature", "pressure", "humidity", "type").filter(join("type") === "mill")
    val saw = join.select("current", "temperature", "pressure", "humidity", "type").filter(join("type") === "saw")
    val laser = join.select("current", "temperature", "pressure", "humidity", "type").filter(join("type") === "laser")

    val corrCurrentTempLathe = lathe.stat.corr("current", "temperature");
    println("Corr between Current vs Temperature for Lathe Machine: %s".format(corrCurrentTempLathe))
    
    val corrCurrentPresuureLathe = lathe.stat.corr("current", "pressure");
    println("Corr between Current vs Pressure for Lathe Machine: %s".format(corrCurrentPresuureLathe))
 
    val corrCurrentHumidityLathe = lathe.stat.corr("current", "humidity");
    println("Corr between Current vs Humidity for Lathe Machine: %s".format(corrCurrentHumidityLathe))
 
  }
}