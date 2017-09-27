package com.DataMach.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession

object CommonFriend3 {
  def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("CommonFriends")
      .getOrCreate()
      
      var file = session.sparkContext.textFile("D:\\PangeaProduct\\Deployment\\data\\FPGrowthData\\friends.txt", 1)
      var file_1 = file.map { x => x.split(" ") }.map { x => (x(0),x(1).split(",")) }.persist()
      var file_2 = file.map { x => x.split(" ") }.map { x => (x(0),x(1).split(",")) }.flatMapValues { x => x }
      var file_3 = file_1.join(file_2).map(f => ((f._1,f._2._2),f._2._1)).map(f => (sortPairAsc(f._1._1,f._1._2),f._2.toSet))
      var file_4 = file_3.aggregateByKey(List[Set[String]]())(_ :+ _, _ ++ _).filter(f => !(f._2.length ==1)).map(f => (f._1,commonValues(f._2)))
      file_4.foreach(println)
      
  }
  
  def commonValues(x:List[Set[String]]):Set[String] = {
    var ret_value = Set[String]()
    for (i <- 0 to x.length-2){
      if (i==0){
        ret_value = x(i)
      }
      ret_value = ret_value.intersect(x(i+1))
    }
    ret_value
  }
  def sortPairDesc(x1:String,x2:String):(String,String) = {
    var ret_value:(String,String) = null
    if (x1(0) >= x2(0)){
      ret_value = (x1,x2)
    }else {
      ret_value = (x2,x1)
    }
    ret_value
  }
  def sortPairAsc(x1:String,x2:String):(String,String) = {
    var ret_value:(String,String) = null
    if (x1(0) <= x2(0)){
      ret_value = (x1,x2)
    }else {
      ret_value = (x2,x1)
    }
    ret_value
  }
}