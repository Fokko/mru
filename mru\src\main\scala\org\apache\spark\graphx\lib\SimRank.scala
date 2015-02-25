package org.apache.spark.graphx.lib

import org.apache.spark.rdd._
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._

/* Implementation of Bipartite SimRank algorithm. */
object SimRank {

  /**
   * Implementation of Bipartite SimRank based on "SimRank:
   * A Measure of Structural-Context Similarity",
   * available at [[http://ilpubs.stanford.edu:8090/508/1/2001-41.pdf]].
   *
   * @param relations RDD of (srcId, dstId) represents the relationship of srcId and dstId.
   *
   * @param C1 Decay factor between 0 and 1 for computing source nodes' similarity.
   *
   * @param C2 Decay factor between 0 and 1 for computing destination nodes' similarity.
   *
   * @param iterN iteration times.
   *
   * @return RDD of (type, obj1Id, obj2Id, sim), type 0 for source nodes,
   *         type 1 for destination nodes, obj1Id and obj2Id are Ids of object1 and object2,
   *         sim is the similarity of object1 and object2.
   */
  def run(relations: RDD[(Long, Long)], C1: Double, C2: Double, iterN: Int)
    : RDD[(Int, Long, Long, Double)] = {

    relations.cache()

    val srcN = relations.map { case (s, d) => s }.distinct.count
    val dstN = relations.map { case (s, d) => d }.distinct.count

    val edges = relations.cartesian(relations).map { case((s1, d1), (s2, d2)) => {
      val s = if (s1 > s2) (s2, s1) else (s1, s2)
      val d = if (d1 > d2) (d2, d1) else (d1, d2)
      (s, d)
    }}.filter { case((s1, s2), (d1, d2)) => s1 != s2 || d1 != d2
    }.map { r =>(r, 1L) }.reduceByKey(_ + _).map { case(r, c) => (r, c / 2)
    }.map{case(((s1, s2), (d1, d2)), c) => Edge((s1 * srcN + s2) * 2,(d1 * dstN + d2) * 2 + 1, c)}


    // Construct graph with vertex id for relation, attribute for similarity
    var g: Graph[Double, Long] = Graph.fromEdges(edges, 0.0).cache()

    def genSimF(srcN: Long, dstN: Long)(vid: VertexId, vd: Double): Double = {
      var sim = 0.0
      if (vid % 2 == 0) {
        val srcVid = vid / 2
        val s1 = srcVid / srcN
        val s2 = srcVid % srcN
        if (s1 == s2) sim = 1.0
      } else {
        val dstVid = vid / 2
        val d1 = dstVid / dstN
        val d2 = dstVid % dstN
        if (d1 == d2) sim = 1.0
      }
      sim
    }

    // Initialize similarity
    g = g.mapVertices(genSimF(srcN, dstN))

    for (i <- 0 until iterN) {
      // Gather adjacent nodes' similarity
      g.cache()
      g = g.mapReduceTriplets(et => Iterator(
        (et.srcId, (et.dstAttr * et.attr, et.attr)),
        (et.dstId, (et.srcAttr * et.attr, et.attr))
        ),
        (a: (Double, Long), b: (Double, Long)) => (a._1 + b._1, a._2 + b._2),
        (vid: VertexId, vd: Double, msg: Option[(Double, Long)]) => {
          var C = 0.0
          if (vid % 2 == 0) C = C1 else C = C2
          if (vd == 1.0) 1.0 else C * msg.get._1 / msg.get._2
        }
      )
    }

    val simRst = g.vertices.map { case(vid, sim) =>
      if ( vid % 2 == 0) {
        val srcVid = vid / 2
        (0, srcVid / srcN, srcVid % srcN, sim)
      } else {
        val dstVid = vid / 2
        (1, dstVid / dstN, dstVid % dstN, sim)
      }
    }
    simRst
  }
}
