package DetectCommunities

import java.io._
import scala.reflect.{ClassTag, classTag}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.graphx._

class NewmanGirvanHarness() {
  private var resultsFile: File = _
  private var resultsFileWriter: PrintWriter = _

  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long], graphName: String): Unit = {
    this.resultsFile = new File(s"results\\results_newman_girvan_$graphName.txt")
    this.resultsFileWriter = new PrintWriter(new FileWriter(this.resultsFile), true)

    if (this.resultsFile.exists()){
      this.resultsFile.delete()
    }
    this.resultsFileWriter.println("-" * 100)
    this.resultsFileWriter.println(s"| Newman-Girvan results ($graphName)" + (" " * (73 - graphName.length)) + "|")
    this.resultsFileWriter.println("-" * 100)

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

      println(s"numberOfComponents = $numberOfComponents, modularity = $modularity")
      if (numberOfComponents >= 2 && modularity > lastModularity){
        println("numberOfComponents >= 2 && modularity < lastModularity")
        // Get connected components
        val cc = gnGraph
          .subgraph(epred = (et) => (et.srcId < et.dstId))
          .connectedComponents()
        println("obtained connected components")
        //map removed edges from current vertexId to connected vertexId
        // 1) extract vertexId from removed edges (src, dst)
        // 2) filter vertices in graph based on vertexId match
        // 3) create map old vertexId -> new vertexId
        // 4) map removed edges from old vertexId -> new vertexId
        // 5) filter edges srcId != dstId
        // 6) aggregate equivalent edges
        // 7) map edge values to relative value

        val vertexIdSet = removedEdgeSet.flatMap(triplet => Set(triplet.srcId, triplet.dstId))
        println("obtained vertexIdSet")
        val vertexIdMap =
          cc.vertices
            .filter(pair => vertexIdSet.contains(pair._1))
            .collectAsMap
        println("obtained vertexIdMap")
        val ccRemovedEdgeList =
          removedEdgeSet.toList
            .map(triplet => (vertexIdMap(triplet.srcId), vertexIdMap(triplet.dstId)))
            .filter(pair => pair._1 != pair._2)
        println("ccRemovedEdgeList")
        val finalEdgeList:List[((VertexId, VertexId), Double)] =
          ccRemovedEdgeList
            .groupBy(pair => pair)
            .mapValues(list => list.length.toDouble/ccRemovedEdgeList.length.toDouble)
            .toList
        println("finalEdgeList")
        //get unique vertexIds
        // 1) map vertices to RDD[VertexId] containing connected component eq class rep
        // 2) count by value Map[VertexId, Long]
        // 3) map to relative values
        val vertexMapping =
          cc.vertices
            .map(pair => pair._2)
            .countByValue
            .toMap
        println("vertexMapping")
        val totalValue = vertexMapping.values.reduce(_ + _).toDouble
        println("totalValue")
        val vertexMappingRelative: Map[VertexId, Double] =
          vertexMapping.mapValues(cnt => cnt.toDouble / totalValue)
        println("vertexMappingRelative")
        //get largest connected component graph and continue that
        // 1) filter cc graph by largest vertexId via subgraph
        // 2) do inner joins on both vertex and edge RDD
        // 3) set filter graph to new graph (including reverse edges)

        val largestCCVertexId =
          vertexMapping.toList
            .sortBy(pair => pair._2)
            .reverse
            .head._1
        println("largestCCVertexId")
        val filteredCCgraph =
          cc.subgraph(vpred = (vid, attr) => (attr == largestCCVertexId))
        println("filtereCCgraph")
        val filteredVertices =
          gnGraph.vertices.innerJoin(filteredCCgraph.vertices)((vid, oldAttr, newAttr) => oldAttr)
        println("filteredVertices")
        val filteredEdges =
          gnGraph.edges.innerJoin(filteredCCgraph.edges)((vid1, vid2, oldAttr, newAttr) => oldAttr)
        println("filteredEdges")
        filteredGraph = Graph(filteredVertices, filteredEdges ++ filteredEdges.reverse)

        println("Biggest graph is " + largestCCVertexId + " with " + filteredGraph.edges.count + " edges.")
        resultsFileWriter.write("\n***********************************\n" + i + "\n")
        resultsFileWriter.write("{\n\"id\" : " + i + ",\n\"nodes\" : [\n")
        val verticesString: String = vertexMappingRelative.toList.foldLeft("")((str, pair) => str + "{\"id\": " + pair._1 + ", \"value\" : " + pair._2 + " }\n")
        resultsFileWriter.write(verticesString)
        resultsFileWriter.write("],\n\"edges\" : [\n")
        val edgesString = finalEdgeList.foldLeft("") ((str, pair) => str + "{\"src_id\": " + pair._1._1 + ", \"dst_id\": " + pair._1._2 + ", \"value\" : " + pair._2 + " }\n")
        resultsFileWriter.write(edgesString)
        resultsFileWriter.write("],\n}\n")

        lastModularity = -1.0
        removedEdgeSet = Set()

        resultsFileWriter.write("\n***********************************\n" + i + "\n")
        resultsFileWriter.write("Biggest graph is " + largestCCVertexId + " with " + filteredGraph.edges.count + " edges.")
      } else {
        println("else")
        lastModularity = modularity
        removedEdgeSet = removedEdgeSet + topEdge
      }

      gnGraph.unpersistVertices(blocking = false)
      gnGraph.edges.unpersist(blocking = false)

      i += 1
    }

    resultsFileWriter.close()
  }
}