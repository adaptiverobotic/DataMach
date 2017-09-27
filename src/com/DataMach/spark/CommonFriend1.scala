package com.DataMach.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession
/* CommonFriend1 - this algorithm computes common friend between couple of individual where the data is provided in below order -
 * 
 * 100 200,300,400,500
	 200 100,300,400
   300 100,200,400,500
   400 100,200,300
 * 
 * It has application in Social networking products where common friends can be used to provide some recommendations
 * 
 * Author - Feroz Khan 
 * */

object CommonFriend1 {
  def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("CommonFriends1")
      .getOrCreate()
      
      var file = session.sparkContext.textFile("D:\\PangeaProduct\\Deployment\\data\\FPGrowthData\\friends.txt", 1)
      file.foreach { println}
      // rdd of friends, list of friends
      var x = file.map { x => (x.split(" ")(0),x.split(" ")(1).split(",")) }.map(f => (f._1,f._2.toList))
      // flatmapvalues to find pair rdd and inversion 
      var y = file.map { x => (x.split(" ")(0),x.split(" ")(1)) }.flatMapValues { x => x.split(',') }.map(_.swap)
      // creating list of friends on reverse 
      var z = y.groupByKey().map(f => (f._1,f._2.toList)) 
      //var x = file.map { x => (x.split(" ")(0),x.split(" ")(1)) }//.map(f => (f._1,generateCombination(f._2)))
      // combining initial rdd and above rdd to ensure we have all individual as keys
      x = x union z
      // removing duplicate keys
      x = x.reduceByKey((x1,x2) => x1++x2).map(f => (f._1,f._2.distinct))
      // finding pair of keys - where keys are not equal
      var a = x.cartesian(x).filter{case (a,b) => a != b}.map{case (a,b)=> ((a._1,b._1),a._2.intersect(b._2))}
      a.foreach(println)
     
  }
  
  def returnList(x:String,y:String):List[String] = {
    var return_value = List[String]()
    return_value = x::return_value
    return_value = y::return_value
    return_value
  }
  
  
  
}