package DetectCommunities

import java.io._

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrameReader

object DetectCommunities {
  /*
  val testDatasetFilename = "TestDataset\\test_graph.csv"
  val caselawDatasetFilename = "CaselawDataset\\citations.csv"
  val twitterDatasetFilename = "TwitterDataset\\users.csv"
  val facebookDatasetFilename = "FacebookDataset\\pages.csv" */
  val testDatasetFilename = "s3://kulalssn/TestDataset/test_graph.csv"
  //val caselawDatasetFilename = "s3://kulalssn/TestDataset/test_graph.csv"
  val caselawDatasetFilename = "s3://kulalssn/CaselawDataset/citations.csv"
  val twitterDatasetFilename = "s3://kulalssn/TwitterDataset/users.csv"
  val facebookDatasetFilename = "s3://kulalssn/FacebookDataset/pages.csv"

  var ss: SparkSession = _;

  def main(args: Array[String]) {
    val resultsDirectory: File = new File("results")

    if (!resultsDirectory.exists()) resultsDirectory.mkdir()

    println("-" * 100)

    this.ss = SparkSession.builder.appName("Community detection").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()

    val t1 = System.nanoTime
    val caselaw = loadCaselaw(ss)
    louvain(caselaw, "caselaw")
    //newmanGirvan(caselaw, "caselaw")
    val twitter = loadTwitter(ss)
    louvain(twitter, "twitter")
    //newmanGirvan(twitter, "twitter")
    val facebook = loadFacebook(ss)
    louvain(facebook, "facebook")
    //newmanGirvan(facebook, "facebook")

    this.ss.stop()

  	println("-" * 100)
  }

  def loadCaselaw(ss: SparkSession): Graph[Null, Long] = {
  	import ss.implicits._
  	val sc = ss.sparkContext
  	val graphData = ss.read.textFile(path = this.caselawDatasetFilename)
	  val graphList = graphData.map(x => {
      x.split(",").map(_.toLong)
    })
	  val edges = graphList.flatMap(x => x.drop(1).map(y => (x(0), y))).map(x => Edge(x._1, x._2, 1L))
    val vertices = graphList.flatMap(x => x).map(x => (x, null)).distinct.sort()
	  val default = null

    Graph[Null, Long](vertices.rdd, edges.rdd, default)
  }

  def loadTwitter(ss: SparkSession): Graph[Null, Long] = {
    import ss.implicits._
    val sc = ss.sparkContext
    val graphData = ss.read.textFile(path = this.twitterDatasetFilename)
    val graphList = graphData.map(x => {
      x.split(", ").map(_.toLong)
    })
    val edges = graphList.flatMap(x => x.drop(1).map(y => (x(0), y))).map(x => Edge(x._1, x._2, 1L))
    val vertices = graphList.flatMap(x => x).map(x => (x, null)).distinct.sort()
    val default = null

    Graph[Null, Long](vertices.rdd, edges.rdd, default)
  }

  def loadFacebook(ss: SparkSession): Graph[Null, Long] = {
    import ss.implicits._
    val sc = ss.sparkContext
    val graphData = ss.read.textFile(path = this.facebookDatasetFilename)
    val graphList = graphData.map(x => {
      x.split(", ").map(_.toLong)
    })
    val edges = graphList.flatMap(x => x.drop(1).map(y => (x(0), y))).map(x => Edge(x._1, x._2, 1L))
    val vertices = graphList.flatMap(x => x).map(x => (x, null)).distinct.sort()
    val default = null

    Graph[Null, Long](vertices.rdd, edges.rdd, default)
  }

  def louvain(graph: Graph[Null, Long], graphName: String): Unit = {
    val lh = new LouvainHarness(1, 10)
    lh.run[Null](this.ss.sparkContext, graph, graphName)
  }

  def newmanGirvan(graph: Graph[Null, Long], graphName: String): Unit = {
    val ngh = new NewmanGirvanHarness()
    ngh.run[Null](this.ss.sparkContext, graph, graphName)
  }
}