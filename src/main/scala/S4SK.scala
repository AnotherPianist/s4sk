import org.apache.spark.{SparkConf, SparkContext}

import java.lang.Long.{bitCount, max}
import scala.util.Random

object S4SK {
  def main(args: Array[String]): Unit = {
    val scale = if (args.length > 0) args(0).toInt else 23
    val rootPath = if (args.length > 1) args(1) else System.currentTimeMillis().toString
    val path = s"$rootPath/s4sk/scale$scale/${System.currentTimeMillis().toString}"
    val numVertices = math.pow(2, scale).toLong
    val numEdges = 16 * numVertices

    val (a, b, c, d) = (0.57d, 0.19d, 0.19d, 0.05d)

    @inline val abcd = Array.tabulate(scale + 1)(x => math.pow(a + b, scale - x) * math.pow(c + d, x))
    val random = new Random()

    val conf = new SparkConf().setAppName("S4SK")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println(s"Creating a graph of 2^$scale ($numVertices) vertices...")

    val startTime = System.currentTimeMillis()

    sc.range(0, numVertices)
      .map(vertexId => (vertexId, getDegree(vertexId, abcd, numEdges, random)))
      .flatMap { case (u, degree) => createDirectedEdges(u, degree) }
      .map { case (u, v) => s"$u\t$v" }
      .saveAsTextFile(path)

    val timeSpent = System.currentTimeMillis() - startTime

    val log =
      s"""================================================================================
         |S4SK
         |Scale $scale
         |Generation completed in ${timeSpent / 1000d} seconds
         |================================================================================""".stripMargin
    println(log)
    sc.parallelize(log, 1).saveAsTextFile(s"$path/logs")

    sc.stop()
  }

  def getDegree(vertexId: Long, abcd: Array[Double], numEdges: Long, random: Random): Int = {
    val expectedDegree = numEdges * abcd(bitCount(vertexId))
    max(0, math.round(expectedDegree + math.sqrt(expectedDegree * (1 - abcd(bitCount(vertexId)))) * random.nextGaussian)).toInt
  }

  def createDirectedEdges(source: Long, degree: Int): Array[(Long, Long)] = {
    val edges = new Array[(Long, Long)](degree)
    var i: Int = 0
    var target: Long = source - 1
    while (i < degree && target >= 0) {
      edges(i) = (source, target)
      i += 1
      target -= 1
    }
    target = source + 1
    while (i < degree) {
      edges(i) = (source, target)
      i += 1
      target += 1
    }
    edges
  }
}
