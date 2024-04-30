package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
  var g = g_in.mapVertices((_, _) => (scala.util.Random.nextDouble(), -1))
  
  var activeVertices = g.vertices.filter(_._2._2 == -1).count()
  var iterations = 0
  val startTimeMillis = System.currentTimeMillis()

  while (activeVertices > 0) {
    iterations += 1

    g = g.mapVertices((_, attr) => (scala.util.Random.nextDouble(), attr._2))

    // Aggregate messages to find the maximum random value from each vertex's neighbors
    val maxNeighborRndMessages = g.aggregateMessages[Double](
      triplet => {
        triplet.sendToDst(triplet.srcAttr._1)
        triplet.sendToSrc(triplet.dstAttr._1)
      },
      (a, b) => math.max(a, b)
    )

    g = g.outerJoinVertices(maxNeighborRndMessages) {
      case (vid, (rnd, status), Some(maxNeighborRnd)) =>
        if (status == -1) {
          if (rnd > maxNeighborRnd) {
            (rnd, 1) // Mark vertex as part of the MIS
          } else {
            (rnd, 0) // Mark vertex as not part of the MIS
          }
        } else {
          (rnd, status)
        }
      case (vid, (rnd, status), None) =>
        (rnd, 1)
    }

    val neighborsInMIS = g.aggregateMessages[Int](
      triplet => {
        if (triplet.srcAttr._2 == 1) triplet.sendToDst(1)
        if (triplet.dstAttr._2 == 1) triplet.sendToSrc(1)
      },
      (a, b) => a + b
    )

    g = g.outerJoinVertices(neighborsInMIS) {
      case (vid, (rnd, status), Some(count)) =>
        if (status == 0 && count > 0) {
          (rnd, 0) // Remove vertex
        } else {
          (rnd, status)
        }
      case (vid, (rnd, status), None) =>
        (rnd, status)
    }

    activeVertices = g.vertices.filter(_._2._2 == -1).count()
  }

  val endTimeMillis = System.currentTimeMillis()
  val durationSeconds = (endTimeMillis - startTimeMillis) / 1000.0
  println(s"LubyMIS completed in $durationSeconds seconds over $iterations iterations.")

  g.mapVertices((_, attr) => attr._2)
}

def verifyMIS(g: Graph[Int, Int]): Boolean = {
  val independent = g.triplets.flatMap { triplet =>
    if ((triplet.srcAttr == 1 && triplet.dstAttr == 1)) {
      Some((triplet.srcId, triplet.dstId))
    } else None
  }.count() == 0

  val maximal = g.vertices.leftOuterJoin(g.aggregateMessages[Int](
    triplet => {
      if (triplet.srcAttr == 1) triplet.sendToDst(1)
      if (triplet.dstAttr == 1) triplet.sendToSrc(1)
    },
    (a, b) => a + b
  )).map {
    case (id, (label, Some(count))) => label != -1 || count > 0
    case (id, (label, None)) => label != -1
  }.reduce(_ && _)

  independent && maximal
}

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans)
        println("Yes")
      else
        println("No")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}


//edges
