package com.DataMach.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("WordCount")
      .getOrCreate()

    var file = session.sparkContext.textFile("D:\\PangeaProduct\\Deployment\\data\\FPGrowthData\\friends.txt", 1)
    //var file_1 = file.map { line => line.split(" ")(1) }
    var file_1 = file.map {split_data}
    var file_2 = file_1.map { x => x.split(",") }
    var file_3 = file_2.flatMap { x => x }.map { x => (x,1) }
    var file_4 = file_3.reduceByKey((x1,x2) => (x1+x2))
    file_1.foreach { println }
    file_4.foreach { println }

  }
  def split_data(x:String):String = {
    x.split(" ")(1)
  }
}