package com.DataMach.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession
import breeze.linalg._
import breeze.numerics._

object LinearRegressionLeastSquare {
  
  def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("LinearRegressionLeastSquare")
      .getOrCreate()
      
      var file = session.sparkContext.textFile("D:\\Users\\mohammad_k\\Documents\\datatMach\\linear_regression.txt", 2)
      var data_rdd = file.map { x => (x.split(',')) }.map { x => (x(0).toInt,x(1).toInt) }.persist()
      // rdd of predictors values 
      var x_rdd = data_rdd.map(f => Array(1.toDouble,f._1.toDouble))
      // rdd of response
      var y_rdd = data_rdd.map(f => f._2)
      // create matrix of predictors 
      var xTx_rdd = x_rdd.zip(x_rdd)
      var partial_matrix = xTx_rdd.map{ case (colA: Array[Double] , rowB:Array[Double]) => var par_mat = breeze.linalg.DenseMatrix.zeros[Double](colA.size , rowB.size)
        
        for (i <- 0 to colA.size-1){
              par_mat(i,::) := new DenseVector(rowB.map { x => x*colA(i) }).t
           }  
        par_mat
      }
      val xTx_matrix = partial_matrix.reduce(_+_)
      val xTx_matrix_inverse = inv(xTx_matrix)
      
      for (i <- 0 to xTx_matrix_inverse.rows-1 ){
        
        var temp_rdd = data_rdd.map(f =>  (f._1*xTx_matrix_inverse(i,0) +f._2*xTx_matrix_inverse(i,1)))
        
        
      }
      
      
      
      
  } 
}