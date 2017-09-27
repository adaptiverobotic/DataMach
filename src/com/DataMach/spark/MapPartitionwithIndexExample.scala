package com.DataMach.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession

object MapPartitionwithIndexExample {
  def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("CommonFriends")
      .getOrCreate()

    val ts = session.sparkContext.parallelize(0 to 100, 10)
    val window = 3

    val partitioned = ts.mapPartitionsWithIndex((i, p) => {
      val overlap = p.take(window - 1).toArray
      val spill = overlap.iterator.map((i - 1, _))
      val keep = (overlap.iterator ++ p).map((i, _))
      println("calling partition " + i)
        overlap.foreach { println }
        println("printing spill")
        spill.foreach(println)
        println("printing keep")
        keep.foreach(println)
        
      if (i == 0) keep else keep ++ spill
    })
    partitioned.collect().foreach(println)
    /*
    var rdd1 = session.sparkContext.parallelize(List(
      "yellow", "red",
      "blue", "cyan",
      "black"),
      3)
    val mapped = rdd1.mapPartitionsWithIndex { (index, iterator) =>
      {
        
        var window = 3
        val overlap = iterator.take(window - 1).toArray
        val spill = iterator.map((index - 1, _))
        val keep = (overlap.iterator ++ iterator).map((index, _))
        println("calling partition " + index)
        overlap.foreach { println }
        println("printing spill")
        spill.foreach(println)
        println("printing keep")
        keep.foreach(println)
        
        iterator.toList.map { x => x + "->" + index }.iterator
      }
    }
    mapped.collect().foreach { println }
    
    */
  }
  class StraightPartitioner(p: Int) extends org.apache.spark.Partitioner {
  def numPartitions = p
  def getPartition(key: Any) = key.asInstanceOf[Int]
}

}