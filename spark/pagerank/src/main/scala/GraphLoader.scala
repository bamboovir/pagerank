import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GraphLoader {
  def edgeListFile(sparkSession: SparkSession, path: String): RDD[(Long, Long)] = {
    val edgesLines = sparkSession.read.textFile(path).rdd
    val edges = edgesLines.map({ line =>
      val lineArray = line.split("\\s+")
      if (lineArray.length < 2) {
        throw new IllegalArgumentException("Invalid line: " + line)
      }
      val srcId = lineArray(0).toLong
      val dstId = lineArray(1).toLong
      (srcId, dstId)
    })
    edges
  }

  def vertexListFile(sparkSession: SparkSession, path: String): RDD[Long] = {
    val vertexLines = sparkSession.read.textFile(path).rdd
    val vertices = vertexLines.map(_.toLong)
    vertices
  }
}
