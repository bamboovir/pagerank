import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.graphx.{Edge, Graph, VertexRDD}
import org.apache.spark.sql.SparkSession

import scala.util.Using

object GraphXPageRank {
  private val USAGE = "Usage: GraphXPageRank <verticesFilePath> <edgesFilePath> <tolerance=1e-8> <dampingFactor=0.85> <topK=10>"

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(USAGE)
      System.exit(1)
    }

    val verticesPath = args(0)
    val edgesPath = args(1)
    val tolerance = if (args.length > 2) args(2).toDouble else 1e-8
    val dampingFactor = if (args.length > 3) args(3).toDouble else 0.85
    val topK = if (args.length > 4) args(4).toInt else 10
    val randomResetProbability = 1 - dampingFactor

    Using(
      SparkSession
        .builder
        .appName("GraphXPageRank")
        .getOrCreate()
    ) { spark =>
      spark.sparkContext.setLogLevel("ERROR")

      val vertices = GraphLoader.vertexListFile(spark, verticesPath).map(x => (x, 1)).cache()
      val edges = GraphLoader.edgeListFile(spark, edgesPath).map {
        case (srcID, dstID) => Edge(srcID, dstID, 1)
      }.cache()
      val graph = Graph(vertices = vertices, edges = edges).cache()

      val ranks = spark.time(PageRank.runUntilConvergence(graph, tolerance, resetProb = randomResetProbability).vertices)
      val topKRanks = ranks.sortBy(_._2, ascending = false).take(topK)
      topKRanks.foreach {
        case (vertexID, rank) => {
          println(s"vertex_id: ${vertexID}\trank: ${rank%.5f}")
        }
      }
    }
  }
}
