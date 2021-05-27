package DetectCommunities

import java.io._
import scala.reflect.{ClassTag, classTag}

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._

class NewmanGirvanHarness() {
  private var resultsFileWriter: BufferedOutputStream = _

  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long], graphName: String): Unit = {
    this.resultsFileWriter = new BufferedOutputStream(FileSystem.get(sc.hadoopConfiguration).create(new Path(s"results/results_newman_girvan_$graphName.txt")))

    this.writeLineToOutputStream("-" * 100)
    this.writeLineToOutputStream(s"| Newman-Girvan results ($graphName)" + (" " * (73 - graphName.length)) + "|")
    this.writeLineToOutputStream("-" * 100)

    val cleanedGraph: Graph[VD, Long] = graph.groupEdges((e1, e2) => e1)
    var filteredGraph: Graph[VD, Double] = cleanedGraph.mapEdges(edge => 0.0)
    filteredGraph.cache()
    var topEdge: EdgeTriplet[VD, Double] = null
    var lastModularity: Double = -1.0

    var i: Long = 0
    var numberOfGraphs: Long = 10
    var removedEdgeSet: Set[EdgeTriplet[VD,Double]] = Set()
    while(!filteredGraph.edges.isEmpty && numberOfGraphs > 0) {
      val gnGraph = NewmanGirvanCore.computeBetweennessGraph(filteredGraph)
      gnGraph.cache()

      filteredGraph.unpersistVertices(blocking = false)
      filteredGraph.edges.unpersist(blocking = false)

      val sortedEdges =
        gnGraph
          .triplets
          .sortBy(triplet => triplet.attr, false)

      topEdge = sortedEdges.first
      sortedEdges.unpersist(blocking = false)

      // Filter out any edges within .1% of the topEdge
      val difference = .999
      filteredGraph = gnGraph
        .subgraph(epred = (et) => {
          et.attr < difference*topEdge.attr
        })

      // Compute modularity
      val pair = NewmanGirvanCore.computeModularity(filteredGraph)
      val numberOfComponents = pair._1
      val modularity = pair._2

      if (numberOfComponents >= 2 && modularity > lastModularity){
        // Get connected components
        val cc = gnGraph
          .subgraph(epred = (et) => (et.srcId < et.dstId))
          .connectedComponents()
        //map removed edges from current vertexId to connected vertexId
        // 1) extract vertexId from removed edges (src, dst)
        // 2) filter vertices in graph based on vertexId match
        // 3) create map old vertexId -> new vertexId
        // 4) map removed edges from old vertexId -> new vertexId
        // 5) filter edges srcId != dstId
        // 6) aggregate equivalent edges
        // 7) map edge values to relative value

        val vertexIdSet = removedEdgeSet.flatMap(triplet => Set(triplet.srcId, triplet.dstId))
        val vertexIdMap =
          cc.vertices
            .filter(pair => vertexIdSet.contains(pair._1))
            .collectAsMap
        val ccRemovedEdgeList =
          removedEdgeSet.toList
            .map(triplet => (vertexIdMap(triplet.srcId), vertexIdMap(triplet.dstId)))
            .filter(pair => pair._1 != pair._2)
        val finalEdgeList:List[((VertexId, VertexId), Double)] =
          ccRemovedEdgeList
            .groupBy(pair => pair)
            .mapValues(list => list.length.toDouble/ccRemovedEdgeList.length.toDouble)
            .toList
        //get unique vertexIds
        // 1) map vertices to RDD[VertexId] containing connected component eq class rep
        // 2) count by value Map[VertexId, Long]
        // 3) map to relative values
        val vertexMapping =
          cc.vertices
            .map(pair => pair._2)
            .countByValue
            .toMap
        val totalValue = vertexMapping.values.reduce(_ + _).toDouble
        val vertexMappingRelative: Map[VertexId, Double] =
          vertexMapping.mapValues(cnt => cnt.toDouble / totalValue)
        //get largest connected component graph and continue that
        // 1) filter cc graph by largest vertexId via subgraph
        // 2) do inner joins on both vertex and edge RDD
        // 3) set filter graph to new graph (including reverse edges)

        val largestCCVertexId =
          vertexMapping.toList
            .sortBy(pair => pair._2)
            .reverse
            .head._1
        val filteredCCgraph =
          cc.subgraph(vpred = (vid, attr) => (attr == largestCCVertexId))
        val filteredVertices =
          gnGraph.vertices.innerJoin(filteredCCgraph.vertices)((vid, oldAttr, newAttr) => oldAttr)
        val filteredEdges =
          gnGraph.edges.innerJoin(filteredCCgraph.edges)((vid1, vid2, oldAttr, newAttr) => oldAttr)
        filteredGraph = Graph(filteredVertices, filteredEdges ++ filteredEdges.reverse)

        this.writeLineToOutputStream("\n***********************************\n" + i + "\n")
        this.writeLineToOutputStream("{\n\"id\" : " + i + ",\n\"nodes\" : [\n")
        val verticesString: String = vertexMappingRelative.toList.foldLeft("")((str, pair) => str + "{\"id\": " + pair._1 + ", \"value\" : " + pair._2 + " }\n")
        this.writeLineToOutputStream(verticesString)
        this.writeLineToOutputStream("],\n\"edges\" : [\n")
        val edgesString = finalEdgeList.foldLeft("") ((str, pair) => str + "{\"src_id\": " + pair._1._1 + ", \"dst_id\": " + pair._1._2 + ", \"value\" : " + pair._2 + " }\n")
        this.writeLineToOutputStream(edgesString)
        this.writeLineToOutputStream("],\n}\n")

        lastModularity = -1.0
        removedEdgeSet = Set()

        this.writeLineToOutputStream("\n***********************************\n" + i + "\n")
        this.writeLineToOutputStream("Biggest graph is " + largestCCVertexId + " with " + filteredGraph.edges.count + " edges.")
      } else {
        println("else")
        lastModularity = modularity
        removedEdgeSet = removedEdgeSet + topEdge
      }

      gnGraph.unpersistVertices(blocking = false)
      gnGraph.edges.unpersist(blocking = false)

      i += 1
    }

    this.resultsFileWriter.close()
  }

  private def writeLineToOutputStream(line: String): Unit = {
    this.resultsFileWriter.write((line + "\n").getBytes("UTF-8"))
  }
}