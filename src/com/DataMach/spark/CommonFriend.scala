package com.DataMach.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession

object CommonFriend {
  def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("CommonFriends")
      .getOrCreate()
      
      var file = session.sparkContext.textFile("D:\\PangeaProduct\\Deployment\\data\\FPGrowthData\\friends.txt", 1)
      file.foreach { println}
      var x = file.map { x => (x.split(" ")(0),x.split(" ")(1).split(",")) }.map(f => (f._1,generateCombination(f._2)))
      //var x = file.map { x => (x.split(" ")(0),x.split(" ")(1)) }//.map(f => (f._1,generateCombination(f._2)))
      x.foreach(println)
      var map_output = file.map { x => generateMap(x) }
      var map_output_1 = map_output.flatMap(f => f)
      var map_output_2 = map_output_1.groupByKey()
      var reduce_output = map_output_2.map(f => generateCommonFriends(f._1,f._2.toList))
      reduce_output.foreach(println)
      //reduce_output.foreach(println)
  }
  
  def generateCommonFriends(key:List[String],list_friends:List[List[String]]):(List[String],List[String]) = {
    if (!(list_friends.isEmpty || list_friends.length == 1)){
      var a_friendsList = list_friends(0).toSet
      var b_friendsList = list_friends(1).toSet
      var common_friends = a_friendsList.intersect(b_friendsList).toList
      return (key,common_friends)
    }
    return (key, List[String]())
    
  }
  
  def generateMap(x:String):List[(List[String],List[String])] = {
      var return_result = List[(List[String],List[String])]()
        
        var line_array = x.split(" ")
        var person = line_array(0)
        var friends = line_array(1).split(",").toList
        
        if (friends.length == 1 ){
          var key = List[String]()
          key = person::key
          key = friends(0)::key
          key = key.sorted
          return_result =  (key,List[String]()) :: return_result
          return return_result
        }else{
          for  (f <- friends){
            var key = List[String]()
            key =  person::key
            key = f:: key
            key = key.sorted
            return_result =  (key,friends) :: return_result
          }
          return return_result
        }
        return return_result
    }
  
  def generateCombination(array_string:Array[String]) : List[List[String]] = {
    
    var list =  List[List[String]] ()
    
    for (i <-  0 to (array_string.length-1) ){
      
      for (j <-  i+1 to (array_string.length-1) ){
        var return_list_1 = List[String]()
        return_list_1 = array_string(i)::return_list_1
        return_list_1 = array_string(j)::return_list_1
        return_list_1 = return_list_1.sorted
        var return_list_2 = return_list_1.reverse
        list = return_list_1 :: list
        list = return_list_2 :: list
        
        
      }
      
      
    }
    list
    
  }
  
}