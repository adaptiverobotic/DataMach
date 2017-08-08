package com.DataMach.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.SparkSession
/* PageRank - A simple implementation of famous pagerank algo using spark and scala
 * file 1 page_list - details of page and its neighbours 
 * 1	2,3,11,12,13,14
2	1,3,15,16,17
3	1,2,4,5,6
4	3
5	3
6	3
15	2
16	2
17	2
11	1
14	1
12	1
13	1
 * 
 * 
 * file 2 - Page_Rank - page and its rank
 * 1,1
2,1
3,1
4,1
5,1
6,1
11,1
12,1
13,1
14,1
15,1
16,1
17,1
 * 
 * */

object PageRank {
  def main(args: Array[String]) {
    var session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local").config("spark.pangea.ae.LOGGER", true)
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("PageRank")
      .getOrCreate()

    var file1 = session.sparkContext.textFile("D:\\Users\\mohammad_k\\Documents\\datatMach\\page_list.txt", 1)
    var file2 = session.sparkContext.textFile("D:\\Users\\mohammad_k\\Documents\\datatMach\\page_rank.txt", 1)
    // converting the page list into all x <-> y combinations where x and y are pages

    var file1_1 = file1.map { x => x.split('\t') }.map { x => (x(0), x(1).split(',')) }.flatMapValues { x => x }
    var file1_2 = file1_1.map(_.swap)
    file1_1 = (file1_1 union file1_2).distinct()

    // page and its rank in double notation
    var file2_1 = file2.map { x => x.split(',') }.map { x => (x(0), x(1).toDouble) }
    // code to broadcast page rank rdd will go here
    // join to give page, neighbour and page rank
    var temp_rank = file1_1.join(file2_1)
    for (i <- 1 to 10) {
      // counting page instances - this will help to find contribution given by rank / count (neighbour)
      var temp_rank_1 = temp_rank.map(f => (f._1, 1)).reduceByKey((x1, x2) => (x1 + x2))
      // generating new page rank values
      var temp_rank_2 = temp_rank.join(temp_rank_1).map(f => (f._2._1._1, f._2._1._2./(f._2._2))).reduceByKey((x1, x2) => (x1 + x2)).mapValues(x => 0.15 + 0.85 * x)
      // generating new temp_rank combination and re iterating 
      temp_rank = file1_1.join(temp_rank_2)

    }

  }
}