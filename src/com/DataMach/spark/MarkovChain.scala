package com.DataMach.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import com.github.nscala_time.time.Imports._


object MarkovChain {
  def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("markovChain")
      .getOrCreate()
      
    
    var file = session.sparkContext.textFile("D:\\Users\\mohammad_k\\Documents\\datatMach\\markov.txt", 1)
    val T1 = DateTime.parse("2013-01-01")
    val T2 = DateTime.parse("2013-01-10")
   
    var t3 = (T1 to T2).millis/86400000
    println(">>"+t3)
    var file1 = file.map { x => x.split(",") }.map { x => (x(0), ( x(2), x(3))) }
    var file2 = file1.groupByKey().map(f => (f._1, f._2.toList.sortBy(f => f._1)))
    file2.foreach(println)
    //file2.foreach(println)
    var file3 = file2.map(f => (f._1,toStateSequence(f._2) ))
    var file4 = file3.map(f => (generateCombination(f._2.toArray))).flatMap { x => x }.map{x => (list_to_Tuple(x),1)}
    var file5 = file4.reduceByKey((x1,x2)=>(x1+x2))
    var file6 = file5.map(f => (f._1._1,f._2)).reduceByKey((x1,x2)=>(x1+x2))
    var probability_map = file6.collectAsMap()
    var file7 = file5.map(f => return_probablity(f,probability_map.toMap) )
    file3.take(100).foreach(println)
    file5.take(100).foreach(println)
    file7.take(100).foreach(println)
    // left is the task to generate markov transtition table 

  }
  
  def return_probablity (x:((String,String),Int),y:Map[String,Int]):((String,String),Double) = {
    var denominator = y(x._1._1)
    var numerator = x._2
    println(denominator+" "+x._2)
    var probaility:Double = numerator.toFloat/denominator 
    println(probaility)
    ((x._1._1,x._1._2),probaility)
  }
  
  def list_to_Tuple(s:List[String]): (String,String) = {
    var ret_value = (s(0),s(1))
        ret_value
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
  def toStateSequence(x: List[(String, String)]): List[String] = {
    
    var stateSequence = List[String]()
    if (x.length == 2) {
      return null
    } else {
      var prior = x(0)
      println("prior"+prior._1+" "+prior._2)
      for (i <- 1 to x.length - 1) {
        var current = x(i)
        var prior_amount = prior._2.toInt
        var prior_date = DateTime.parse(prior._1)
        var current_amount = current._2.toInt
        var current_date = DateTime.parse(current._1)
        var amount_diff = current_amount - prior_amount
        var date_diff = (prior_date to current_date).millis/(86400000)

        var dd = "";
        if (date_diff < 30) {
          dd = "S";
        } else if (date_diff < 60) {
          dd = "M";
        } else {
          dd = "L";
        }
        
        var ad = "";
        if (prior_amount < 0.9 * current_amount) {
          ad = "L";
        } else if (current_amount < 1.1 * current_amount) {
          ad = "E";
        } else {
          ad = "G";
        }
        
        var element = dd + ad
        stateSequence = element :: stateSequence
        prior = current
      }
    }
        stateSequence
  }
}