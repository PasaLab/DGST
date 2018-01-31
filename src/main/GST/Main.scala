package GST

import gct.rdd.{GctConf, SplitParallelLcpMergeTree}
import org.apache.spark.{SparkConf, SparkContext}
import util.file.CleanDir

/**
 * DGST Algorithm implemented in the project
 */
object Main {

  def main(args: Array[String]): Unit = {

    val conf = new GctConf

    conf.inputDir = args(0)
    conf.outputLocation = args(1)
    conf.workingDir = args(2)

    if (args.length >= 4)
      conf.range = args(3).toInt
    if (args.length >= 5)
      conf.maxCount = args(4).toInt
    if (args.length >= 6)
      conf.partition = args(5).toInt

    val sparkConf = new SparkConf().setAppName("DGST tree")

    sparkConf.set("spark.executor.cores", "1")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrationRequired", "true")
    sparkConf.set("spark.kryo.registrator", classOf[kryo.Register].getName)

    val sc = new SparkContext(sparkConf)

    var startTime = System.nanoTime()

    var endTime = System.nanoTime()
    CleanDir.clean(conf.workingDir)
    CleanDir.clean(conf.outputLocation)
    conf.disableSegment = true
    startTime = System.nanoTime()
    val tree = new SplitParallelLcpMergeTree(conf)
    tree.build(sc)
    endTime = System.nanoTime()
    println("======================================")
    println("my, time: " + (endTime - startTime) / 1000000 + "ms")
    println("======================================")
  }
}
