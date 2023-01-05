import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet
import org.apache.hadoop.io.LongWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object SimpleTG_NSKG extends Serializable {
  def main(args: Array[String]): Unit = {
    val scale = if (args.length > 0) args(0).toInt else 20
    val rootPath = if (args.length > 1) args(1) else System.currentTimeMillis().toString
    val path = s"$rootPath/simpletg-nskg/scale$scale/${System.currentTimeMillis().toString}"
    val machines = if (args.length > 2) args(2).toInt else 8
    val noise = if (args.length > 3) args(3).toDouble else 0.1d
    val ratio = if (args.length > 4) args(4).toInt else 16
    val (a, b, c, d) = (0.57d, 0.19d, 0.19d, 0.05d)


    val numVertices = math.pow(2, scale).toInt
    val numEdges = ratio * numVertices

    val rng: Long = System.currentTimeMillis

    println(s"Probabilities=($a, $b, $c, $d), |V|=$numVertices (2 ^ $scale), |E|=$numEdges ($ratio * $numVertices)")
    println(s"PATH=$path, Machine=$machines")
    println(s"RandomSeed=$rng")

    val conf = new SparkConf().setAppName("SimpleTG NSKG")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val startTime = System.currentTimeMillis()

    val vertexRDD = sc.range(0, numVertices - 1, numSlices = machines)
    val ds = sc.broadcast(new NSKG(scale, ratio, noise, a, b, c, d))
    val degreeRDD = vertexRDD.map(vertexId => (vertexId, ds.value.getExpectedDegree(vertexId)))
    val partitionedVertexRDD = degreeRDD.rangePartition(machines, numVertices, numEdges)
    val edges = partitionedVertexRDD.doRecVecGen(ds, rng)
    edges.saveAsHadoopFile(path, classOf[LongWritable], classOf[LongOpenHashBigSet], classOf[TSVOutputFormat])

    val timeSpent = System.currentTimeMillis() - startTime

    val log =
      s"""================================================================================
         |SimpleTG NSKG
         |Scale $scale
         |Generation completed in ${timeSpent / 1000d} seconds
         |================================================================================""".stripMargin
    println(log)
    sc.parallelize(log, 1).saveAsTextFile(s"$path/logs")

    sc.stop
  }

  implicit class RecVecGenClass(self: RDD[Long]) extends Serializable {
    def doRecVecGen(ds: Broadcast[_ <: SKG], rng: Long): RDD[(Long, LongOpenHashBigSet)] = {
      self.mapPartitions { partitions =>
        val nskg = ds.value
        partitions.flatMap { u =>
          val random = new Random(rng + u)
          val degree = nskg.getDegree(u, random)
          if (degree < 1)
            Iterator.empty
          else {
            val recVec = nskg.getRecVec(u)
            val sigmas = nskg.getSigmas(recVec)
            val adjacency = new LongOpenHashBigSet(degree)
            var i = 0
            while (i < degree) {
              adjacency.add(nskg.determineEdge(recVec, sigmas, random))
              i += 1
            }
            Iterator((u, adjacency))
          }
        }
      }
    }
  }

  implicit class RangePartitionFromDegreeRDD(self: RDD[(Long, Double)]) extends Serializable {
    def rangePartition(numMachines: Int, numVertices: Long, numEdges: Long): RDD[Long] = {
      val sc = self.sparkContext
      val accumulator = new SetAccumulatorV2()
      sc.register(accumulator, "SetAccumulatorV2")
      val lastGlobal = self.fold((0L, 0d)) { (left, right) =>
        val first = if (left._1 > right._1) right else left
        val second = if (left._1 > right._1) left else right
        if (first._2 > (numEdges / numMachines / 100)) {
          accumulator.add(first)
          second
        } else (second._1, first._2 + second._2)
      }
      accumulator.add(lastGlobal)
      val sorted = accumulator.value.toSeq.sortBy { case (vid, _) => vid }
      val range = for (i <- 0 until sorted.length - 1)
        yield if (sorted(i)._1 <= sorted(i + 1)._1 - 1)
          (sorted(i)._1, sorted(i + 1)._1 - (if (i == sorted.length - 2) 0 else 1))
        else (-1L, -1L)
      val range2 = range.filter(p => p._1 >= 0 && p._2 >= 0).zipWithIndex
      val range2finalize = range2.map { case ((f, s), i) => if (i == range2.length - 1) ((f, numVertices - 1), i) else ((f, s), i) }
      val range3 = range2finalize.map { case ((st, ed), _) => (st, ed) }
      val rangeRDD = sc.parallelize(range3, numMachines)
      val threshold = (Int.MaxValue / 4).toLong
      val rangeRDD2 = rangeRDD.flatMap {
        case (f, s) =>
          val end = math.ceil((s - f + 1).toDouble / threshold.toDouble).toInt
          if (end == 1) Iterable((f, s))
          else {
            val array = new Array[(Long, Long)](end)
            var i = 0
            while (i + 1 < end) {
              array(i) = (f + threshold * i, f + threshold * (i + 1) - 1)
              i += 1
            }
            array(end - 1) = (f + threshold * (end - 1), s)
            array
          }
      }.flatMap(x => x._1 to x._2)
      rangeRDD2
    }
  }

  class SetAccumulatorV2(initialValue: Set[(Long, Double)] = Set((0L, 0d))) extends AccumulatorV2[(Long, Double), Set[(Long, Double)]] {
    private var set: Set[(Long, Double)] = initialValue

    override def isZero: Boolean = set.size == 1

    override def copy(): AccumulatorV2[(Long, Double), Set[(Long, Double)]] = new SetAccumulatorV2(set)

    override def reset(): Unit = { set = Set((0L, 0d)) }

    override def add(v: (Long, Double)): Unit = set += v

    override def merge(other: AccumulatorV2[(Long, Double), Set[(Long, Double)]]): Unit = set ++ other.value

    override def value: Set[(Long, Double)] = set
  }

}
