package com.DataMach.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ StructType, StructField, StringType }
/*Author - Feroz Khan
 * This code is an attempt to generate Gini index for categorical column values and suggest appropriate split value. split value will be helpful in creating 
 * a decision tree model
 * */

object Gini {
  def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("Gini")
      .getOrCreate()

    var file = session.sparkContext.textFile("D:\\Users\\mohammad_k\\Documents\\datatMach\\gini.txt", 1)
    var file_1 = file.map { x => (x.split('\t')) }.map { x => Row.fromSeq(x.toSeq) }
    val newstructure = StructType(Seq(StructField("Gender", StringType, true), StructField("Age", StringType, true), StructField("Default", StringType, true)))
    var newdataset = session.sqlContext.createDataFrame(file_1, newstructure)
    //newdataset.registerTempTable("tab")
    //session.sqlContext.sql("select default, count(*)*100/(select count(*) from tab) as default_count from tab where gender = 'Male' group by default").show()
    var inputColumn = "Gender"
    var labelColumn = "Default"

    println("Gini value is " + computeGini(newdataset, inputColumn, labelColumn))
  }

  def computeGini(df: DataFrame, inputCol: String, labelCol: String): Double = {

    var return_value = 0.0
    var parent_gini = 1.0
    
    var gini_map = Map[String, Double]()
    
    val total = df.count().toInt
    df.persist()
    var parent_rdd = df.select(labelCol).rdd.map { x => (x.toString(), 1) }.reduceByKey((x1, x2) => (x1 + x2))
     parent_rdd.foreach { f =>
        parent_gini -= (f._2.toFloat / total) * (f._2.toFloat / total)
        println("outside gini" + parent_gini)    
      }
    
    
        
    var inputColValues = df.select(inputCol).distinct().collect().map { x => x.getString(0) }
    for (i <- 0 to inputColValues.length - 1) {
      println("inputColValues(i)" + inputColValues(i))
      var left_child_gini = 1.0
      var right_child_gini = 1.0
      val left_total = df.filter(df(inputCol) === inputColValues(i)).count().toInt
      val right_total = total-left_total
      
      
      var left_child_rdd = df.filter(df(inputCol) === inputColValues(i)).select(labelCol).rdd.map { x => (x.toString(), 1) }.reduceByKey((x1, x2) => (x1 + x2))
        left_child_rdd.foreach { f =>
          left_child_gini -= (f._2.toFloat / left_total) * (f._2.toFloat / left_total)
          println("inside gini" + (left_total.toFloat / total))
          println("inside gini" + left_child_gini * (left_total.toFloat / total))
          
        }
      
        var right_child_rdd = df.filter(df(inputCol) !== inputColValues(i)).select(labelCol).rdd.map { x => (x.toString(), 1) }.reduceByKey((x1, x2) => (x1 + x2))
        right_child_rdd.foreach { f =>
          right_child_gini -= (f._2.toFloat / right_total) * (f._2.toFloat / right_total)
          println("inside gini" + (right_total.toFloat / total))
          println("inside gini" + right_child_gini * (right_total.toFloat / total))
          
        }
        
        gini_map += (inputColValues(i) ->  (parent_gini -left_child_gini- right_child_gini))
      
    }
    
    
    return_value
  }

}