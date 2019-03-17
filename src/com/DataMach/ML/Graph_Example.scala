package com.DataMach.ML

import edu.umd.cloud9.collection.XMLInputFormat
import org.apache.spark.sql.{ Dataset, SparkSession,Row }
import org.apache.hadoop.io.{ Text, LongWritable }
import org.apache.hadoop.conf.Configuration
import scala.xml._
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import org.apache.spark.graphx._


object Graph_Example {
  
  def main(args: Array[String]) {
    val session = SparkSession.builder()
      // uncomment below line for local testing 
      .master("local")
      .config("spark.sql.warehouse.dir", "TestResult")
      .appName("Graph")
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    import session.implicits._

    val medlineRaw = loadMedLine(session, "C:/Users/feroz/Documents/Spark-ML/data/GraphAnalysis/medsamp2016a.xml/")
    /*printing medline raw records*/
    //medlineRaw.show(10,false)

    val medline: Dataset[Seq[String]] = medlineRaw.map(majorTopics).cache()
    println("printing topics >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    medline.show(10,false)

    /*Simply printing the count of various keywords that are added as citations */

    val topics = medline.flatMap(f => f).toDF("topic")
    topics.createOrReplaceTempView("topics")
    println("printing topic and topic count >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val topicDist = session.sql("""select topic, count(*) as topic_count from topics group by topic order by topic_count desc""")
    topicDist.show(false)

    val topicPairs = medline.flatMap(t => {
      t.sorted.combinations(2)
    }).toDF("pairs")
    topicPairs.createOrReplaceTempView("topic_pairs")
    val co_occurs = session.sql("""
                              SELECT pairs, COUNT(*) cnt
                              FROM topic_pairs
                              GROUP BY pairs order by cnt desc limit 10""")

   println("printing topic pairs and their count >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>") 
   co_occurs.show(false)
    
   val vertices = topics.map{case Row(topic:String) => (hashId(topic),topic)}.toDF("hash","topic")
    
   val vertexRDD = vertices.rdd.map{
      case Row(hash: Long, topic: String) => (hash, topic)
   }
   println("printing VertexRDD >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>") 
   
   vertexRDD.take(2).foreach(f=> println(f._1+" "+f._2))
    
   val edges = co_occurs.map{ case Row(topics: Seq[_], cnt: Long) =>
      val ids = topics.map(_.toString).map(hashId).sorted
      Edge(ids(0), ids(1), cnt)
    }
   println("printing Edge Dataset >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>") 
    
   edges.show(10)
    val topicGraph = Graph(vertexRDD, edges.rdd)
    topicGraph.cache()
    
    val connected_components = topicGraph.connectedComponents().vertices.toDF("vId","cid")
    val componentCounts = connected_components.groupBy("cid").count()
    println("printing connected component id  their count >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>") 
    componentCounts.orderBy("count").show(false)
    
    
    val topicComponentDF = topicGraph.vertices.map(f => (f._1,f._2)).join(topicGraph.connectedComponents().vertices.map(x => (x._1,x._2))).
    map(x => (x._2._1,x._2._2)).toDF("name","cid" )
    
    topicComponentDF.show(false)
    
    
  }
  
  def loadMedLine(spark: SparkSession, path: String) = {
    import spark.implicits._
    val conf = new Configuration()
    conf.set(XMLInputFormat.START_TAG_KEY, "<MedlineCitation ")
    conf.set(XMLInputFormat.END_TAG_KEY, "</MedlineCitation>")
    val sc = spark.sparkContext
    val in = sc.newAPIHadoopFile(path, classOf[XMLInputFormat], classOf[LongWritable], classOf[Text], conf)
    in.map(f => f._2.toString()).toDS()

  }

  def majorTopics(record: String): Seq[String] = {
    val elem = XML.loadString(record)
    val dn = elem \\ "DescriptorName"
    val mt = dn.filter(n => (n \ "@MajorTopicYN").text == "Y")
    mt.map(n => n.text)
  }

  def hashId(str: String): Long = {
    val bytes = MessageDigest.getInstance("MD5").
      digest(str.getBytes(StandardCharsets.UTF_8))
    (bytes(0) & 0xFFL) |
      ((bytes(1) & 0xFFL) << 8) |
      ((bytes(2) & 0xFFL) << 16) |
      ((bytes(3) & 0xFFL) << 24) |
      ((bytes(4) & 0xFFL) << 32) |
      ((bytes(5) & 0xFFL) << 40) |
      ((bytes(6) & 0xFFL) << 48) |
      ((bytes(7) & 0xFFL) << 56)
  }
  
  

}