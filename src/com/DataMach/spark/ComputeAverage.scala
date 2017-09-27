package com.DataMach.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession
/* ComputeAverage - Using Aggregate function to compute average
 * 
 * Demonstration of aggregate by function to compute average of numbers
 * 
 * Author - Feroz Khan 
 * */

object ComputeAverage {
  def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("CommonFriends1")
      .getOrCreate()
      
      var file = session.sparkContext.textFile("D:\\Users\\mohammad_k\\Documents\\datatMach\\compute_average.txt")
      println(1%2)
      println(0%2)
      println(3%2)
      var file1 = file.map { x => x.split(',') }.map { x => ( x(0) , x(1).toDouble ) }
      //var file2 = file1.mapValues { x => (x.toDouble,1) }
      var file2 = file1.aggregateByKey((0.0,0))(  (x, y) => (x._1 + y, x._2 + 1), (x,y) =>(x._1 + y._1, x._2 + y._2))
      //var file3 = file2.reduceByKey{(v1,v2) => (v1._1 + v2._1 , v1._2 + v2._2)}.map{f => (f._1,f._2._1./(f._2._2))}
      //file3.foreach(println)
     file2.foreach(println)
  }
  
  
  
  
  
}