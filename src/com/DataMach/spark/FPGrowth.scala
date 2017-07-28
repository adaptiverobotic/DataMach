package com.DataMach.spark

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.reflect.runtime.universe
import org.apache.spark.sql.SparkSession
import util.control.Breaks._

object FPGrowth {

  def main(args: Array[String]) {

    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("FPGrowthManual")
      .getOrCreate()
    var dataFields = "transaction id,item"
    var schemasave = StructType(dataFields.split(",").map(fieldName => StructField(fieldName, StringType, false)))
    var dataset = session.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("D:\\PangeaProduct\\Deployment\\data\\FPGrowthData\\BMS3.csv") //.schema(schemasave)
    //dataset.show(10)
    //dataset.registerTempTable("transaction")
    //var dataset1 = session.sqlContext.sql("select * from transaction where transaction.`transaction id` = 28")
    //dataset1.show(10)
    var datasetRDD = dataset.rdd.map { case Row(x: String, y: String) => (x, y) }
    // transaction and all the items in a list
    var datasetRDD_aggregatebyTransaction = datasetRDD.groupByKey()
    datasetRDD_aggregatebyTransaction.foreach(println)
    // mapping all the item for counting purpose 
    var datasetRDD_aggregatebyTransaction_1 = datasetRDD_aggregatebyTransaction.map(f => (f._1,findSortedCombinations(f._2.toList))).flatMapValues { x => x }.map(f => (f._2,1))
    datasetRDD_aggregatebyTransaction_1.foreach(println)
    // reducing item and getting item count
    var datasetRDD_aggregatebyTransaction_2 =  datasetRDD_aggregatebyTransaction_1.reduceByKey((v1,v2) => (v1+v2))
        datasetRDD_aggregatebyTransaction_2.foreach(println)
    var datasetRDD_aggregatebyTransaction_3 = datasetRDD_aggregatebyTransaction_2.map(f => (subPattern(f._1,f._2))).flatMap(f => f)
        datasetRDD_aggregatebyTransaction_3.foreach(println)
    var datasetRDD_aggregatebyTransaction_4 = datasetRDD_aggregatebyTransaction_3.groupByKey()
    datasetRDD_aggregatebyTransaction_4.foreach(println)
    var datasetRDD_aggregatebyTransaction_5 = datasetRDD_aggregatebyTransaction_4.map(f => (generateAssociationRule(f._1,f._2.toList)))
    datasetRDD_aggregatebyTransaction_5.foreach(println)
  }

  var new_list = List("a","b","c")
  val output = removeOneItem(new_list,1)
  val output2 = removeOneItem(new_list,2)
  val output0 = removeOneItem(new_list,0)
  
  println (">>>>>>"+output+output2+output0 )
  
  //println(output)
  
  def findSortedCombinations(elements: List[String]): List[List[String]] = {
    var result = List[List[String]]()
    var i = 1
    elements.foreach { x =>
      result = result ++ findSortedCombinations(elements, i)
      i = i + 1
    }
    //var return_result = result.map { x => ("["+x.mkString(",")+"]") }
    return result;
  }

  def findSortedCombinations(elements: List[String], n: Int): List[List[String]] = {
    var result = List[List[String]]()
    if (n == 0) {
      result = List[String]() :: result 
      return result
    }
    
    var combination: List[List[String]] = findSortedCombinations(elements, n - 1)
    
    combination.foreach { x =>
      elements.foreach { y =>
        breakable {
          if (x.contains(y)) { break }
          var ls = List[String]()
          ls = ls ++ x
          if (ls.contains(y)) { break }
          ls = y :: ls
          ls = ls.sortBy { x => x.toString() }
          if (result.contains(ls)) { break }
          result = ls :: result
      
        }
      }
    }
    result
    
  }
  
  def toList(transaction:String) : List[String] = {
    transaction.split(",").toList
  }
  
  def removeOneItem(list_of_items:List[String], n:Int) : List[String] = {
    if ( (n < 0) || (n >= list_of_items.length) || (list_of_items == null || list_of_items.isEmpty)){
      list_of_items
    }
    var clone = list_of_items
    clone.patch(n, Nil, 1)
  }
  
  def subPattern (ls:List[String],freq:Integer) : List[(List[String],(List[String],Integer))] = {
    var result = List[(List[String],(List[String],Integer))]()
    result = (ls,(null,freq))::result
    if (ls.size == 1){
      return result
    }
    
    for (i <- 0 to (ls.size-1) ){
      
      var sublist = removeOneItem(ls,i)
      result = (sublist,(ls,freq))::result
    }
    result
  }
  def generateAssociationRule(pattern_list:List[String],subpattern_list:List[(List[String],Integer)]):List[(List[String],List[String],Double)]={
    var return_value = List[(List[String],List[String],Double)]()
    var to_list = List[(List[String],Integer)]()
    var from_count:(List[String],Integer) = null
    var to = subpattern_list
    var confidence:Double = 0.0
    for ((pattern,freq) <- to){
      if (pattern == null){
        from_count = (pattern,freq)
      }else{
        to_list = (pattern,freq)::to_list
      }
      
    }
    if (to_list.isEmpty){
      return return_value
    }
    for ((pattern,freq) <- to_list){
      confidence = freq.toDouble/from_count._2.toDouble
      var list = pattern
      list = list.filterNot { pattern_list.toSet }
      return_value = (pattern_list,list,confidence) :: return_value
    }
    return_value
  }
  
}
  
  
  
  