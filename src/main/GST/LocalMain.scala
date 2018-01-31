
package GST

import gct.rdd.{EraTree, GctConf, SplitParallelLcpMergeTree}
import org.apache.spark.{SparkConf, SparkContext}
import util.file.CleanDir

/**
 * Local Main for Debug
 */
object LocalMain {

  def main(args: Array[String]): Unit = {

    val conf = new GctConf

    val ITEM = args(0).toInt
    conf.alphabetNum = ITEM

    val budget = args(1).toInt
    conf.maxCount = budget

    val bs = args(2).toInt
    conf.startLen = bs

    val step = args(3).toFloat
    conf.step = step

    conf.partition = 40

    conf.range = args(4).toInt

    val option = args(5)

    conf.workingDir = "D:\\tmp"

    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.1\\hadoop-2.7.1")
    val sparkConf = new SparkConf().setAppName("DGST")
    sparkConf.setMaster("local")

    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrationRequired", "true")
    sparkConf.set("spark.kryo.registrator", classOf[kryo.Register].getName)

    conf.inputDir = "D:\\test"
    conf.localInputDir = conf.inputDir

    conf.sortingBufferSize = 20000000

    val sc = new SparkContext(sparkConf)

    val input = sc.wholeTextFiles(conf.inputDir).map(x => (x._2.getBytes, x._1.substring(1 + x._1.lastIndexOf('/'))))
    input.values.collect().foreach(println)

    var startTime = System.nanoTime()
    var endTime = System.nanoTime()

    CleanDir.clean(conf.workingDir)

    val eraFlag = option.toLowerCase match {
      case "era" => true
      case "both" => true
      case _ => false
    }
    val lmFlag = option.toLowerCase match {
      case "lm" => true
      case "both" => true
      case _ => false
    }

    if (eraFlag) {
      conf.outputLocation = "D:\\earoutput"
      CleanDir.clean(conf.outputLocation)
      conf.disableSegment = true
      startTime = System.nanoTime()
      val tree = new EraTree(conf)
      tree.build(input)
      endTime = System.nanoTime()
      println("======================================")
      println("era, time: " + (endTime - startTime) / 1000000 + "ms")
      println("======================================")
    }

    if (lmFlag) {
      conf.outputLocation = "D:\\output"
      conf.disableSegment = true
      CleanDir.clean(conf.outputLocation)
      startTime = System.nanoTime()
      val tree = new SplitParallelLcpMergeTree(conf)
      tree.build(sc)
      endTime = System.nanoTime()
      println("======================================")
      println("my, time: " + (endTime - startTime) / 1000000 + "ms")
      println("======================================")
    }
  }

}
