package com.DataMach.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession
import scala.math.sqrt
import org.apache.spark.HashPartitioner

object RecommendationEngine {
    def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("RecommendationEngine")
      .getOrCreate()
      
      var file = session.sparkContext.textFile("D:\\PangeaProduct\\Deployment\\data\\TestData\\als.csv", 1)
                  .map { x => x.split(',') }.map { x => (x(2),(x(1),x(3).toInt)) }
                  .groupByKey.map(f => ((f._1,f._2.size),f._2))
                  .flatMapValues(x => x)
                  .map(f => (f._2._1,(f._1._1,f._2._2,f._1._2))).partitionBy(new HashPartitioner(4)).persist()
     file.foreach(println)             
     //var file4  = file3.join(file3).map(f => orderList(f._2) ).distinct().filter(f => f._1._1 != f._2._1).map(f => ((f._1._1,f._2._1),(f._1._2,f._1._3,f._2._2,f._2._3,f._1._2.toInt.*(f._2._2.toInt),f._1._2.toInt.*(f._1._2.toInt),f._2._2.toInt.*(f._2._2.toInt))))
     var file1  = file.join(file).map(f => orderList(f._2) ).distinct().filter(f => f._1._1 != f._2._1).map(f => ((f._1._1,f._2._1),(f._1._2.toInt,f._1._3.toInt,f._2._2.toInt,f._2._3.toInt))).repartition(4).groupByKey.mapValues(f => f.toList)
     //file1.foreach(println)
     var file2 = file1.map(f => (f._1, computePearsonCorrelation(f._2)))
     file2.foreach(println)
      
    }
    
    def computePearsonCorrelation(x_list:List[(Int,Int,Int,Int)]): Double={
      var return_value:Double = 0.0
      var sum_xy = 0
      var sum_x = 0
      var sum_y = 0
      var n = x_list.length
      var sum_x_sq = 0
      var sum_y_sq = 0
      
      for (x <- x_list){
        sum_xy += x._1*x._3
        sum_x +=x._1
        sum_y +=x._3
        sum_x_sq += x._1.*(x._1)
        sum_y_sq += x._3.*(x._3)
      }
      
      
      return_value = (n.*(sum_xy) - sum_x.*(sum_y))./(sqrt((n.*(sum_x_sq) - sum_x.*(sum_x)).toDouble).*(sqrt((n.*(sum_y_sq) - sum_y.*(sum_y)).toDouble)))
      
      return_value
    }
    
    
    def orderList(x:((String,Int,Int),(String,Int,Int))) : ((String,Int,Int),(String,Int,Int)) ={
      var return_value:((String,Int,Int),(String,Int,Int)) = null 
      if (x._1._1.toInt > x._2._1.toInt){
        return_value =  (x._2,x._1)
      }else {
        return_value =  (x._1,x._2)
      }
      return_value
    }
}