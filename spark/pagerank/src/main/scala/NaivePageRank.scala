import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Using

object NaivePageRank {
  private val USAGE = "Usage: NaivePageRank <verticesFilePath> <edgesFilePath> <tolerance=1e-8> <dampingFactor=0.85> <topK=10>"

  def pageRank(
                sparkSession: SparkSession,
                verticesFilePath: String,
                edgesFilePath: String,
                tolerance: Double = 1e-8,
                dampingFactor: Double = 0.85
              ): RDD[(Long, Double)] = {
    val vertices = GraphLoader.vertexListFile(sparkSession, verticesFilePath).cache()
    val edges = GraphLoader.edgeListFile(sparkSession, edgesFilePath).cache()
    val randomResetProbability = 1 - dampingFactor

    val zeroOutDegreeVertices = vertices.subtract(edges.map(_._1))
    val graph = edges.groupByKey().++(zeroOutDegreeVertices.map(x => (x, Seq()))).cache()
    val baseContributions = vertices.map(x => (x, 0.0)).cache()
    var ranks = vertices.map(x => (x, 1.0))
    var maxRankDiff = Double.MaxValue
    var iterationTimesToConvergence = 0

    while (maxRankDiff >= tolerance) {
      iterationTimesToConvergence += 1
      val contributions = graph.join(ranks).values
        .flatMap {
          case (destinations, rank) =>
            val outDegree = destinations.size
            destinations.map(dst => (dst, rank / outDegree))
        }

      val nextRanks = (baseContributions.++(contributions))
        .reduceByKey(_ + _)
        .mapValues(randomResetProbability + dampingFactor * _)

      maxRankDiff = ranks.join(nextRanks).values
        .map { case (prevRank, currRank) => (currRank - prevRank).abs }
        .max()

      ranks = nextRanks
    }

    println(s"iteration_times_to_convergence: $iterationTimesToConvergence")
    normalizeRankSum(ranks)
  }

  def normalizeRankSum(ranks: RDD[(Long, Double)]): RDD[(Long, Double)] = {
    val rankSum = ranks.map(_._2).sum()
    val numOfNodes = ranks.count()
    val correctionFactor = numOfNodes / rankSum
    ranks.mapValues(_ * correctionFactor)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(USAGE)
      System.exit(1)
    }

    val verticesPath = args(0)
    val edgesPath = args(1)
    val tolerance = if (args.length > 2) args(2).toDouble else 1e-8
    val dampingFactor = if (args.length > 3) args(3).toDouble else 0.85
    val topK = if (args.length > 4) args(4).toInt else 10

    Using(
      SparkSession
        .builder
        .appName("NaivePageRank")
        .getOrCreate()
    ) { spark =>
      spark.sparkContext.setLogLevel("ERROR")
      val ranks = spark.time(pageRank(spark, verticesPath, edgesPath, tolerance, dampingFactor))
      val topKRanks = ranks.sortBy(_._2, ascending = false).take(topK)
      topKRanks.foreach {
        case (vertexID, rank) => {
          println(s"vertex_id: ${vertexID}\trank: ${"%.5f".format(rank)}")
        }
      }
    }
  }
}
