package org.apache.spark.graphx.lib

import org.scalatest.FunSuite

import org.apache.spark.graphx._

class SimRankSuite extends FunSuite with LocalSparkContext {

  test("Test SimRank") {
    withSpark { sc =>
      val relations = sc.textFile("graphx/data/sim_rank.data").map { line =>
        val fields = line.split(",")
        (fields(0).toLong, fields(1).toLong)
      }
      val simRst = SimRank.run(relations, 0.8, 0.8, 10).collect

      for (i <- 0 until simRst.size) {
        val t = simRst(i)._1
        val obj1Id = simRst(i)._2
        val obj2Id = simRst(i)._3
        val sim = simRst(i)._4

        if (t == 0 && obj1Id == 0 && obj2Id == 0) assert(sim == 1.0)
        if (t == 0 && obj1Id == 0 && obj2Id == 1) assert(Math.abs(sim - 0.54) < 0.01)
        if (t == 0 && obj1Id == 1 && obj2Id == 1) assert(sim == 1.0)
        if (t == 1 && obj1Id == 1 && obj2Id == 1) assert(sim == 1.0)
        if (t == 1 && obj1Id == 2 && obj2Id == 2) assert(sim == 1.0)
        if (t == 1 && obj1Id == 0 && obj2Id == 3) assert(Math.abs(sim - 0.43) < 0.01)
        if (t == 1 && obj1Id == 0 && obj2Id == 1) assert(Math.abs(sim - 0.61) < 0.01)
        if (t == 1 && obj1Id == 0 && obj2Id == 2) assert(Math.abs(sim - 0.61) < 0.01)
        if (t == 1 && obj1Id == 1 && obj2Id == 2) assert(Math.abs(sim - 0.61) < 0.01)
        if (t == 1 && obj1Id == 1 && obj2Id == 3) assert(Math.abs(sim - 0.61) < 0.01)
        if (t == 1 && obj1Id == 2 && obj2Id == 3) assert(Math.abs(sim - 0.61) < 0.01)
      }
    }
  }

}
