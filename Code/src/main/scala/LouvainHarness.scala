package DetectCommunities

import java.io._
import scala.reflect.ClassTag

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._

class LouvainHarness(minProgress: Int, progressCounter: Int) {
	private var resultsFileWriter: BufferedOutputStream = _

  def run[VD: ClassTag](sc: SparkContext, graph: Graph[VD, Long], graphName: String): Unit = {
		this.resultsFileWriter = new BufferedOutputStream(FileSystem.get(sc.hadoopConfiguration).create(new Path(s"results/results_louvain_$graphName.txt")))

    this.writeLineToOutputStream("-" * 100)
		this.writeLineToOutputStream(s"| Louvain results ($graphName)" + (" " * (79 - graphName.length)) + "|")
		this.writeLineToOutputStream("-" * 100)

		var louvainGraph = LouvainCore.createLouvainGraph(graph)
    
    var level = -1
    var q = -1.0
	  var halt = false
    do {
	    level += 1

	    val (currentQ, currentGraph, passes) = LouvainCore.louvain(sc, louvainGraph, minProgress, progressCounter)
	    louvainGraph.unpersistVertices(blocking=false)
	    louvainGraph = currentGraph
	  
	    saveLevel(sc, level, currentQ, louvainGraph)

	    if (passes > 2 && currentQ > q + 0.001 ){
	      q = currentQ
	      louvainGraph = LouvainCore.compressGraph(louvainGraph)
	    } else {
	      halt = true
	    }
	 	} while ( !halt )

		this.resultsFileWriter.close()
	}

  private def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[LouvainVertexState, Long]): Unit = {
		val vertices: Array[(VertexId, LouvainVertexState)] = graph.vertices.collect

		this.writeLineToOutputStream(s"Level $level, q = $q, # communities = ${graph.vertices.count}")
		this.writeLineToOutputStream("Vertex\tCommunity\tCommunitySigmaTot\tInternalWeight\tnodeWeight")
		for (vertex <- vertices) this.writeLineToOutputStream(s"${vertex._1}\t${vertex._2.community}\t\t${vertex._2.communitySigmaTot}\t\t\t${vertex._2.internalWeight}\t\t${vertex._2.nodeWeight}")
		this.writeLineToOutputStream("-" * 100)
  }

	private def writeLineToOutputStream(line: String): Unit = {
		this.resultsFileWriter.write((line + "\n").getBytes("UTF-8"))
	}
}