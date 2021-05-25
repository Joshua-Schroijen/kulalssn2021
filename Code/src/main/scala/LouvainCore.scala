package DetectCommunities

import scala.reflect.ClassTag
import scala.math.BigDecimal.double2bigDecimal

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.broadcast.Broadcast

object LouvainCore {
  def createLouvainGraph[VD: ClassTag](graph: Graph[VD, Long]) : Graph[LouvainVertexState, Long] = {
    def nodeWeightMapFunc(e: EdgeContext[VD, Long, Long]) {
      e.sendToSrc(e.attr)
      e.sendToDst(e.attr)
    }
    val nodeWeightReduceFunc = (e1: Long, e2: Long) => e1 + e2
    val nodeWeights = graph.aggregateMessages(nodeWeightMapFunc, nodeWeightReduceFunc)
    
    val louvainGraph = graph.outerJoinVertices(nodeWeights)((vid,data,weightOption) => { 
      val weight = weightOption.getOrElse(0L)
      val state = new LouvainVertexState()
      state.community = vid
      state.changed = false
      state.communitySigmaTot = weight
      state.internalWeight = 0L
      state.nodeWeight = weight
      state
    }).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_+_)
    
    return louvainGraph
  }	

  def louvainFromStandardGraph[VD: ClassTag](sc:SparkContext, graph:Graph[VD, Long], minProgress:Int=1, progressCounter:Int=1) : (Double, Graph[LouvainVertexState, Long], Int) = {
	  val louvainGraph = createLouvainGraph(graph)
	  return louvain(sc, louvainGraph, minProgress, progressCounter)
  }

  def louvain(sc: SparkContext, graph: Graph[LouvainVertexState, Long], minProgress: Int = 1, progressCounter:Int = 1): (Double, Graph[LouvainVertexState, Long], Int) = {
    var louvainGraph = graph.cache()
    val graphWeight = louvainGraph.vertices.values.map(vdata => vdata.internalWeight + vdata.nodeWeight).reduce(_+_)
    var totalGraphWeight = sc.broadcast(graphWeight) 

    var msgRDD = louvainGraph.aggregateMessages(sendMsg, mergeMsg)
    var activeMessages = msgRDD.count() //materializes the msgRDD and caches it in memory
     
    var updated = 0L - minProgress
    var even = false  
    var count = 0
    val maxIter = 100000 
    var stop = 0
    var updatedLastPhase = 0L
    do { 
      count += 1
      even = ! even
	  
      val labeledVerts = louvainVertJoin(louvainGraph, msgRDD, totalGraphWeight, even).cache()
	   
	    val communtiyUpdate = labeledVerts
	      .map( {case (vid,vdata) => (vdata.community,vdata.nodeWeight+vdata.internalWeight)})
	      .reduceByKey(_+_).cache()
	   
	    val communityMapping = labeledVerts
	          .map( {case (vid,vdata) => (vdata.community,vid)})
	          .join(communtiyUpdate)
	          .map({case (community,(vid,sigmaTot)) => (vid,(community,sigmaTot)) })
	          .cache()
	   
	    val updatedVerts = labeledVerts.join(communityMapping).map({
        case (vid, (vdata, communityTuple)) =>
	        vdata.community = communityTuple._1
	        vdata.communitySigmaTot = communityTuple._2
	        (vid,vdata)
      }).cache()
	    updatedVerts.count()
	    labeledVerts.unpersist(blocking = false)
      communtiyUpdate.unpersist(blocking = false)
	    communityMapping.unpersist(blocking = false)
	   
	    val prevG = louvainGraph
	    louvainGraph = louvainGraph.outerJoinVertices(updatedVerts)((vid, old, newOpt) => newOpt.getOrElse(old))
	    louvainGraph.cache()
	   
	    val oldMsgs = msgRDD
      msgRDD = louvainGraph.aggregateMessages(sendMsg, mergeMsg).cache()
      activeMessages = msgRDD.count()  // materializes the graph by forcing computation
	 
      oldMsgs.unpersist(blocking = false)
      updatedVerts.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)

	    if (even) updated = 0
	    updated = updated + louvainGraph.vertices.filter(_._2.changed).count
	    if (!even) {
	      if (updated >= updatedLastPhase - minProgress) stop += 1
	      updatedLastPhase = updated
	    }
    } while (stop <= progressCounter && (even || (updated > 0 && count < maxIter)))

    val newVerts = louvainGraph.vertices.innerJoin(msgRDD)((vid,vdata,msgs) => {
      val community = vdata.community
      var k_i_in = vdata.internalWeight
      var sigmaTot = vdata.communitySigmaTot.toDouble
      msgs.foreach({
        case((communityId, sigmaTotal), communityEdgeWeight) =>
          if (vdata.community == communityId) k_i_in += communityEdgeWeight
      })
      val M = totalGraphWeight.value
      val k_i = vdata.nodeWeight + vdata.internalWeight
      var q = (k_i_in.toDouble / M) - ((sigmaTot * k_i) / math.pow(M, 2))
      if (q < 0) 0 else q
    })  
    val actualQ = newVerts.values.reduce(_+_)

    return (actualQ, louvainGraph, count / 2)
  }

  private def sendMsg(et: EdgeContext[LouvainVertexState, Long, Map[(Long, Long), Long]]): Unit = {
    et.sendToSrc(Map((et.dstAttr.community, et.dstAttr.communitySigmaTot) -> et.attr))
    et.sendToDst(Map((et.srcAttr.community, et.srcAttr.communitySigmaTot) -> et.attr))
  }

  private def mergeMsg(m1:Map[(Long, Long), Long], m2:Map[(Long, Long), Long]) = {
    val newMap = scala.collection.mutable.HashMap[(Long, Long), Long]()
    m1.foreach({
      case (k, v) =>
        if (newMap.contains(k)) newMap(k) = newMap(k) + v
        else newMap(k) = v
    })
    m2.foreach({
      case (k, v) =>
        if (newMap.contains(k)) newMap(k) = newMap(k) + v
        else newMap(k) = v
    })
    newMap.toMap
  }

  private def louvainVertJoin(louvainGraph:Graph[LouvainVertexState,Long], msgRDD:VertexRDD[Map[(Long,Long),Long]], totalEdgeWeight:Broadcast[Long], even:Boolean) = {
    louvainGraph.vertices.innerJoin(msgRDD)((vid, vdata, msgs) => {
      var bestCommunity = vdata.community
		  var startingCommunityId = bestCommunity
		  var maxDeltaQ = BigDecimal(0.0);
      var bestSigmaTot = 0L
      msgs.foreach({
        case((communityId, sigmaTotal), communityEdgeWeight) =>
	      	val deltaQ = q(startingCommunityId, communityId, sigmaTotal, communityEdgeWeight, vdata.nodeWeight, vdata.internalWeight, totalEdgeWeight.value)
	        if (deltaQ > maxDeltaQ || (deltaQ > 0 && (deltaQ == maxDeltaQ && communityId > bestCommunity))){
	          maxDeltaQ = deltaQ
	          bestCommunity = communityId
	          bestSigmaTot = sigmaTotal
	        }
      })
		  if (vdata.community != bestCommunity && ((even && vdata.community > bestCommunity) || (!even && vdata.community < bestCommunity))){
		    vdata.community = bestCommunity
		    vdata.communitySigmaTot = bestSigmaTot  
		    vdata.changed = true
		  } else {
		    vdata.changed = false
		  }
      vdata
    })
  }

  private def q(currCommunityId:Long, testCommunityId:Long, testSigmaTot:Long, edgeWeightInCommunity:Long, nodeWeight:Long, internalWeight:Long, totalEdgeWeight:Long) : BigDecimal = {
	 	val isCurrentCommunity = (currCommunityId.equals(testCommunityId));
		val M = BigDecimal(totalEdgeWeight); 
	  val k_i_in_L = if (isCurrentCommunity) edgeWeightInCommunity + internalWeight else edgeWeightInCommunity;
		val k_i_in = BigDecimal(k_i_in_L);
		val k_i = BigDecimal(nodeWeight + internalWeight);
		val sigma_tot = if (isCurrentCommunity) BigDecimal(testSigmaTot) - k_i else BigDecimal(testSigmaTot);
		
		var deltaQ =  BigDecimal(0.0);
		if (!(isCurrentCommunity && sigma_tot.equals(0.0))) {
			deltaQ = k_i_in - ( k_i * sigma_tot / M)
		}
		return deltaQ;
  }

  def compressGraph(graph:Graph[LouvainVertexState,Long],debug:Boolean=true) : Graph[LouvainVertexState,Long] = {
    val internalEdgeWeights = graph.triplets.flatMap(et=>{
    	if (et.srcAttr.community == et.dstAttr.community){
            Iterator( ( et.srcAttr.community, 2*et.attr) )
          } 
          else Iterator.empty  
    }).reduceByKey(_+_)
     
    var internalWeights = graph.vertices.values.map(vdata=> (vdata.community,vdata.internalWeight)).reduceByKey(_+_)
   
    val newVerts = internalWeights.leftOuterJoin(internalEdgeWeights).map({case (vid,(weight1,weight2Option)) =>
      val weight2 = weight2Option.getOrElse(0L)
      val state = new LouvainVertexState()
      state.community = vid
      state.changed = false
      state.communitySigmaTot = 0L
      state.internalWeight = weight1+weight2
      state.nodeWeight = 0L
      (vid,state)
    }).cache()

    val edges = graph.triplets.flatMap(et=> {
       val src = math.min(et.srcAttr.community,et.dstAttr.community)
       val dst = math.max(et.srcAttr.community,et.dstAttr.community)
       if (src != dst) Iterator(new Edge(src, dst, et.attr))
       else Iterator.empty
    }).cache()

    val compressedGraph = Graph(newVerts,edges)
      .partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_+_)
    
    def nodeWeightMapFunc(e: EdgeContext[LouvainVertexState, Long, Long]) {
      e.sendToSrc(e.attr)
      e.sendToDst(e.attr)
    }
    val nodeWeightReduceFunc = (e1:Long, e2:Long) => e1 + e2
    val nodeWeights = compressedGraph.aggregateMessages(nodeWeightMapFunc,nodeWeightReduceFunc)

    val louvainGraph = compressedGraph.outerJoinVertices(nodeWeights)((vid,data,weightOption)=> { 
      val weight = weightOption.getOrElse(0L)
      data.communitySigmaTot = weight +data.internalWeight
      data.nodeWeight = weight
      data
    }).cache()
    louvainGraph.vertices.count()
    louvainGraph.triplets.count()
    
    newVerts.unpersist(blocking=false)
    edges.unpersist(blocking=false)
    return louvainGraph
  }
}