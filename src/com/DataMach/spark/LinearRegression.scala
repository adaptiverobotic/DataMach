
package com.DataMach.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession

object LinearRegression {
  
  def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("LinearRegression")
      .getOrCreate()
      
      var file = session.sparkContext.textFile("D:\\Users\\mohammad_k\\Documents\\datatMach\\linear_regression.txt", 2)
      var x_rdd = file.map { x => (x.split(',')) }.map { x => (x(0).toInt,x(1).toInt) }.persist()
      
      var x_mean_values = x_rdd.map(f => f._1).aggregate((0,0))((x, y) => (x._1 + y, x._2 + 1), (x,y) =>(x._1 + y._1, x._2 + y._2))
      var y_mean_values = x_rdd.map(f => f._2).aggregate((0,0))((x, y) => (x._1 + y, x._2 + 1), (x,y) =>(x._1 + y._1, x._2 + y._2))
      var x_mean =   x_mean_values._1.toFloat/x_mean_values._2
      var y_mean =   y_mean_values._1.toFloat/y_mean_values._2
      var var_x =  x_rdd.map { x => (x._1 - x_mean)*(x._1 - x_mean) }.reduce( (x,y) => (x+y) )
      var cov_x_y =  x_rdd.map { x => (x._1 - x_mean)*(x._2 - y_mean) }.reduce( (x,y) => (x+y) )
      var B1 = cov_x_y.toFloat /var_x
      var B2 = y_mean - B1* x_mean
      
      println ("Coeff B1 "+B1+" Coeff B2 "+B2)
            
      
  } 
}