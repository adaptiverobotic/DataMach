package com.DataMach.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession

import org.apache.spark.HashPartitioner

object MovingAverages {
  def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("MovingAverages")
      .getOrCreate()

    val ts = session.sparkContext.parallelize(0 to 100, 10)
    val window = 3
    /*
    val partitioned = ts.mapPartitionsWithIndex((i, p) => {
      val overlap = p.take(window - 1).toArray
      val spill = overlap.iterator.map((i - 1, _))
      val keep = p.map((i, _))
      if (i == 0) keep else keep ++ spill
    }).partitionBy(new StraightPartitioner(ts.partitions.length)).values
    partitioned.foreach { println }
    */
    
    val partitioned = ts.mapPartitionsWithIndex((i, p) => {
      val overlap = p.take(window - 1).toArray
      val spill = overlap.iterator.map((i - 1, _))
      val keep = (overlap.iterator ++ p).map((i, _))
      //println("overlap" + overlap.toList)
      //println("spill" + spill.toList)
      //println("keep" + keep.toList)
      //println("p" + p.toList)
      if (i == 0) keep else keep ++ spill
    }).groupByKey().flatMapValues { x => x }.values
    //.partitionBy(new StraightPartitioner(ts.partitions.length))
    //.values
    
    //partitioned.foreach { println }
    val movingAverage = partitioned.mapPartitions(p => { 
      var ls = List[Float]()
      var ls1 = p.toArray
      for (i <- 0 to (ls1.length-window-1)){
        var avg = (ls1(i)+ ls1(i+1) +ls1(i+2)).toFloat./(3)
        ls = avg::ls
      }
    ls.sorted.iterator  
    })
    movingAverage.foreach { println }
    /*
    val movingAverage = partitioned.mapPartitions(p => {
      val sorted = p.toSeq.sorted
      val olds = sorted.iterator
      val news = sorted.iterator
      var sum = news.take(window - 1).sum
      (olds zip news).map({
        case (o, n) => {
          sum += n
          val v = sum
          sum -= o
          v
        }
      })
    })
    //movingAverage.foreach { println }
  */  
  }
  class StraightPartitioner(p: Int) extends org.apache.spark.Partitioner {
    def numPartitions = p
    def getPartition(key: Any) = key.asInstanceOf[Int]
  }

}