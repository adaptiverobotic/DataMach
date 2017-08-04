package com.DataMach.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import com.github.nscala_time.time.Imports._

object NaiveBayes {
  def main(args: Array[String]) { 
  var session = SparkSession.builder()
      
  // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("NaiveBayes")
      .getOrCreate()
      
    
    var file = session.sparkContext.textFile("D:\\Users\\mohammad_k\\Documents\\datatMach\\naive_bayes.txt", 1)
    
    /* Naive Bayes Classifier - is the extension of bayes theorm where we believe each evidence is an independent event 
     * p(outcome|multiple evidence) = p(evidence1|outcome)*p(evidence2|outcome)*....*p(evidenceN|outcome)/p(evidence1)*p(evidence2)*p(evidence3)*..*p(evidenceN)
     * "evidence" are predictors , "outcome" is classification outcome
     * */
    
    var rdd_class_count = file.map { x => x.split(",") }.map { x => x(x.length-1) }.map { x => (("CLASS"+"|"+x),1) }
    // Count of total records - used to calculate probability
    var total_count = rdd_class_count.count()
    var map_class_count = rdd_class_count.reduceByKey((x1,x2)=>(x1+x2))
    var class_count = map_class_count.collectAsMap()
    class_count.foreach(println)
    // map variable containing probability of each outcome
    var probability_class_count = map_class_count.map(f => (f._1,f._2.toFloat./(total_count))).collectAsMap()
    probability_class_count.foreach(println)
    var rdd_attribute_class_count = file.map { x => x.split(",") }.flatMap{x => returnAttributeCombination(x) }.map{x => (x,1)}.reduceByKey((x1,x2) => (x1+x2))
    rdd_attribute_class_count.foreach(println)
    // map variable containing probability of each evidence given outcome
    var prob_attribute_class_count = rdd_attribute_class_count.map(f => (f._1,f._2.toFloat./(class_count("CLASS"+"|"+f._1.split('|')(1))))).collectAsMap()
    var attribute_count = rdd_attribute_class_count.map(f => (f._1.split('|')(0),f._2)).reduceByKey((x1,x2)=>(x1+x2))
    // map variable containing probability of each evidence
    var prob_attribute_count = attribute_count.map(f => (f._1, f._2.toFloat./(total_count))).collectAsMap()
    var input_array = "Sunny,Hot,High,Weak".split(',')
    var output_class = computeNaiveBayes(probability_class_count.toMap,prob_attribute_class_count.toMap,prob_attribute_count.toMap,input_array)
    println(">>"+output_class)
    
  }
  
  def  computeNaiveBayes(probability_class_count:Map[String,Float],prob_attribute_class_count:Map[String,Float],prob_attribute_count:Map[String,Float],input:Array[String]):String ={
    var classification_class = ""
    var output_class_map = scala.collection.mutable.Map[String,Float]()
    for ((k,v) <- probability_class_count){
        classification_class = k.split('|')(1)
        println("classification_class" + classification_class)
        var numerator:Float = v
        var denominator:Float = 1
        for (i <- 0 to input.length-1){
          println(">>>>>>>"+input(i))  
          var prob_value = prob_attribute_class_count.getOrElse(input(i)+"|"+classification_class,0.toFloat)
          
          numerator = numerator.*(prob_value) 
            denominator = denominator* prob_attribute_count(input(i))
        }
        println("numby denom" +numerator.toFloat./(denominator) )
        output_class_map +=(classification_class -> numerator.toFloat./(denominator))
        
      }
    var return_value = output_class_map.maxBy(_._2)
    return_value._1
  }
  
  def returnAttributeCombination(x:Array[String]): List[String] = {
    var return_list = List[String]()
    for (i <-  0 to (x.length-2) ){
      return_list = (x(i)+"|"+x(x.length-1))::return_list
    }
    return_list
  }
}