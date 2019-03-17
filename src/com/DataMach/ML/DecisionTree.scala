package com.DataMach.ML

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._
import org.apache.log4j.{Level, Logger}

object DecisionTree {
  
  def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local")
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("DecisionTree")
      .getOrCreate()
      
      /*logging properties - will attend this shortly*/
      
      session.sparkContext.setLogLevel("ERROR")
      Logger.getLogger("org").setLevel(Level.OFF)
    // the data consist of "forest cover" in usa and various parameters 

    val raw_data = session.sparkContext.textFile("C:\\Users\\feroz\\Documents\\Spark-ML\\data\\covtype.data");
    
    /*lets view data to understand*/
    println(">>>>>>>>>> Printing a sample of data")
    raw_data.take(10).foreach { println }
    
    /*before running decision tree model we will be converting raw data into LabeledPoint object 
       * that will consist of a feature vector and label*/

    val data = raw_data.map { x =>
      val arr_x = x.split(",").map { x => x.toDouble }
      val feature_vector = Vectors.dense(arr_x.init) /*init returns all but last value*/
      val label = arr_x.last - 1
      LabeledPoint(label, feature_vector)

    }
    /*holding out training data, test data and CV data*/

    val Array(training_data, test_data, cv_data) = data.randomSplit(Array(0.8, 0.1, 0.1))
    // caching the data as it will be used in iterative processing 
    training_data.cache()
    cv_data.cache()
    // creating a decision tree model on training data
    val model = org.apache.spark.mllib.tree.DecisionTree.trainClassifier(training_data, 7, Map[Int, Int](), "gini", 4, 100)
    
    // computing multiclass metrics 
    val metrics = getMetrics(model, cv_data)
    
    println(">>>>>>>>>> printing confusion metrics")
    println(metrics.confusionMatrix)
    
    // printing net precision - precision is all true predicted (actually true) divided by predicted as true.
    println(">>>>>>>>>> printing precision and recall value")
    println(metrics.precision)
    // printing net recall - recall is all true predicted values (actually true )divided by all true.
    println(metrics.recall)
    // printing precision and recall value of each label 
    
    (0 until 7).map(
    x => (metrics.precision(x),metrics.recall(x))    
    ).foreach(println)
    
    /* tuning decision tree model - we will be evaulating the performance of decision tree against 
    various hyperparameters combinations that will give us various accuracy measures */
     
    val evaluations = 
    for (impurity <- Array("gini","entropy");depth <- Array(1,20); bins <- Array(10,300)) 
      yield{
      val model = org.apache.spark.mllib.tree.DecisionTree.trainClassifier(training_data, 7, Map[Int, Int](), impurity, depth, bins)
      val prediction_and_label = cv_data.map( x => ( model.predict(x.features),x.label))
      val accuracy = new MulticlassMetrics(prediction_and_label).precision
      ((impurity,depth,bins),accuracy)
    }
    println(">>>>>>>>>> Evaluating various models based on impurity, depth and bin")
    evaluations.sortBy(_._2).reverse.foreach(println)
    

  }
 
  /* function to compute multiclass metrics from prediction and to verify the quality of prediction*/
  
 def getMetrics(model:DecisionTreeModel, data:RDD[LabeledPoint]):MulticlassMetrics = {
       val prediction_and_label = data.map( x => ( model.predict(x.features),x.label))
       new MulticlassMetrics(prediction_and_label)
  } 
 
  
}