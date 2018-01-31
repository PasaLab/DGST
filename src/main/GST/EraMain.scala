package GST

import gct.rdd.{EraTree, GctConf}
import org.apache.spark.{SparkConf, SparkContext}
import util.file.CleanDir

/**
 * ERa Algorithm implemented in the project
 */
object EraMain {
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

    val sparkConf = new SparkConf().setAppName("Era tree")
    sparkConf.set("spark.executor.cores", "1")

    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrationRequired", "true")
    sparkConf.set("spark.kryo.registrator", classOf[kryo.Register].getName)
    //    sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val sc = new SparkContext(sparkConf)
    val input = sc.wholeTextFiles(conf.inputDir).map(x => (x._2.getBytes, x._1.substring(1 + x._1.lastIndexOf(conf.dirSpliter))))

    CleanDir.clean(conf.workingDir)
    CleanDir.clean(conf.outputLocation)
    conf.disableSegment = true
    val startTime = System.nanoTime()
    val tree = new EraTree(conf)
    tree.build(input)
    val endTime = System.nanoTime()
    println("======================================")
    println("era, time: " + (endTime - startTime) / 1000000 + "ms")
    println("======================================")


  }


}
