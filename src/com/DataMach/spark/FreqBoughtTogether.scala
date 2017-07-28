package com.DataMach.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession

object FreqBoughtTogether {
  def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("FBT")
      .getOrCreate()
      
      var file = session.sparkContext.textFile("D:\\Users\\mohammad_k\\Documents\\datatMach\\fbt.txt", 1)
      var file1 = file.map { x => generateCombination(x.split("\t")(1).split(","))}
      file1.foreach { println }
      var file2 = file1.flatMap { x => x }.map(f => (f,1)).reduceByKey((x1,x2) => (x1+x2)).map(f => (f._1(0),(f._1(1),f._2))).groupByKey()
      var file3 = file2.map { x => (x._1,mostFreqList(x._2.toList,2)) }
      file3.foreach(println)
      
  }
  /*
  var list = List ("a","b","c","d").toArray
  var list1 = List (1,2,3,4).toArray
  
  var re_value = generateCombination(list1)
  re_value.foreach(println)
  */
  def mostFreqList(ls:List[(String,Int)],n:Integer): List[String] = {
    var return_result = List[String]()
    var new_ls = ls.sortBy(f => f._2).take(n)
    for (i <- new_ls){
      return_result = i._1::return_result
      
    }
    return_result
    
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
  /*
  def generateList(value1:String,value2:String):List[(String,Int)] = {
    var return_value = List[(String,Int)]()
    
  }*/
  
  def generateCombination(array_string:Array[Int]) : List[List[Int]] = {
    
    var list =  List[List[Int]] ()
    
    for (i <-  0 to (array_string.length-1) ){
      
      for (j <-  i+1 to (array_string.length-1) ){
        var return_list = List[Int]()
        return_list = array_string(i)::return_list
        return_list = array_string(j)::return_list
        return_list = return_list.sorted
        list = return_list :: list
      }
      
      
    }
    list
    
  }
  
  
  
  
  
  
}