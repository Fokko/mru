package org.apache.spark.graphx.lib

import org.scalatest.FunSuite

import org.apache.spark.graphx._


class PLSASuite extends FunSuite with LocalSparkContext {

  test("Test PLSA") {
    withSpark { sc =>
      val edges = sc.textFile("graphx/data/doc_word_count.data").map { line =>
        val fields = line.split(",")
        Edge(fields(0).toLong * 2, fields(1).toLong * 2 + 1, fields(2).toLong)
      }
      var g = PLSA.run(edges, 2, 60) // 2 topics
      var d0 =  g.vertices.filter {case(vid, vd) => vid == 0}.collect
      var d1 =  g.vertices.filter {case(vid, vd) => vid == 2}.collect
      var t0 =  g.vertices.filter {case(vid, vd) => vid == 1}.collect
      var t2 =  g.vertices.filter {case(vid, vd) => vid == 5}.collect
      assert((d0(0)._2.get(0) - d0(0)._2.get(1)) * (t0(0)._2.get(0) - t0(0)._2.get(1)) > 0.0)
      assert((d1(0)._2.get(0) - d1(0)._2.get(1)) * (t2(0)._2.get(0) - t2(0)._2.get(1)) > 0.0)
    }
  }

}
