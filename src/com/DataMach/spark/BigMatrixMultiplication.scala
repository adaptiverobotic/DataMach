
package com.DataMach.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession
//import org.apache.spark.mllib.linalg.{Matrix, Matrices, DenseMatrix, Vector}
import breeze.stats.distributions.Rand
import breeze.linalg._
import breeze.numerics._

/*Multiplying big matrices in Spark - using concept of outer product . We can use this to multiply very large matrices where one 
 * matrix is a wide matrix having large number of columns and other is a long matrix having large number of rows. For wide matrix 
 * we will create rdd of columns which are small in size and will fit memory, long matrix will have rdd of rows which will fit to memory
 * because number of columns of first matrix is equal to number of rows of second matrix, both rdd will contain equal number of elements 
 * and product of elements when outer multiplied together and added will get final multiplication result.
 * */


object BigMatrixMultiplication {
  
  def main(args: Array[String]) {
    // new version added
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("BigMatrixMultiplication")
      .getOrCreate()
      
      //val wideMatrix = new breeze.linalg.DenseMatrix[Double](2, 3, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
      //val longMatrix = new breeze.linalg.DenseMatrix[Double](3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
      
      val n = 10000 // n could be a very large number 
      
      val wideMatrix = DenseMatrix.rand(2, n)
      val longMatrix = DenseMatrix.rand(n,2)
      
      val wide_rdd = session.sparkContext.parallelize(wideMatrix.toArray.sliding(2,2).toSeq)
      val long_rdd = session.sparkContext.parallelize(longMatrix.t.toArray.sliding(2,2).toSeq)
      val zip_rdd = wide_rdd.zip(long_rdd)
      wide_rdd.foreach {x => x.foreach { println }  }
      long_rdd.foreach { z => z.foreach { println } }
      var partial_matrix = zip_rdd.map{ case (colA: Array[Double] , rowB:Array[Double]) => var par_mat = breeze.linalg.DenseMatrix.zeros[Double](colA.size , rowB.size)
        
        for (i <- 0 to colA.size-1){
              par_mat(i,::) := new DenseVector(rowB.map { x => x*colA(i) }).t
           }  
        par_mat
      }
      val full_matrix = partial_matrix.reduce(_+_)
      full_matrix.foreachValue { println }
    
  }
  
}