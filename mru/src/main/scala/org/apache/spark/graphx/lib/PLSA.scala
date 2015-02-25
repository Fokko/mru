
package org.apache.spark.graphx.lib

import scala.util.Random
import org.jblas.DoubleMatrix
import org.apache.spark.rdd._
import org.apache.spark.graphx._

/** Implementation of Probabilistic latent semantic analysis (PLSA) via EM algorithm. */
object PLSA {

  /**
   * @param edges edges for constructing the bipartite graph with source vertex for document
   *        and destination vertex for word, and edge attribute for word occurrence in document.
   * 
   * @param topicN topic number.
   *
   * @param iterN iteration times.
   *
   * @return a graph with vertex attributes containing the document's topic distribution
   *         on document vertex and the topic's word distribution on word vertex.
   */
  def run(edges: RDD[Edge[Long]], topicN: Int, iterN: Int): Graph[DoubleMatrix, Long] =
  {
    def defaultF(topicN: Int) = {
      val arr = new DoubleMatrix(topicN)
      for (i <- 0 until topicN) arr.put(i, Random.nextDouble)
      arr
    }

    var g: Graph[DoubleMatrix, Long] = Graph.fromEdges(edges, defaultF(topicN)).cache()

    g = g.mapVertices((vid: VertexId, vd: DoubleMatrix) => vd.add(Random.nextDouble * 0.5))

    def normF(ts: DoubleMatrix)(vid: VertexId, vd: DoubleMatrix): DoubleMatrix = {
      if (vid % 2 == 0) vd.div(vd.sum) else vd.divColumnVector(ts)
    }

    for (i <- 0 until iterN) {
      // EM step
      g.cache()
      g = g.mapReduceUpdate(et => {
        val msg = et.srcAttr
          .mulColumnVector(et.dstAttr)
          .mul(et.attr / et.srcAttr.dot(et.dstAttr))
        Iterator((et.srcId, msg), (et.dstId, msg))
        },
        (a: DoubleMatrix, b: DoubleMatrix) => a.add(b),
        (vid: VertexId, vd: DoubleMatrix, msg: Option[DoubleMatrix]) => msg.get
      )

      // Normalize probability
      g.cache()
      val ts: DoubleMatrix = g.vertices.filter { case (vid, vd) => vid % 2 == 1 }
        .map { case (vid, vd) => vd }.reduce((a, b) => a.addColumnVector(b))
      g = g.mapVertices(normF(ts))
    }

    g
  }
}
