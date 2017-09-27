package com.DataMach.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.rdd.RDDFunctions._

object MovingAverage {
  def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("MovingAverage")
      .getOrCreate()
      
      
    val ts = session.sparkContext.parallelize(0 to 100, 10).sliding(3)//.map { x => (x.sum/x.size) }.collect()
    ts.take(3)(2).foreach { println }
    
  }
  
}