package valid

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * Validation the accuracy of final suffix tree
 */
object AccuracyValidation {

  def main(args: Array[String]) {

    val correctPath = args(0)
    val judgePath = args(1)
    val partition = args(2).toInt

    val op = if (args.length < 4)
      "line"
    else
      args(3)

    System.setProperty("hadoop.home.dir", "C:\\projects\\hadoop-2.7.1")
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val sparkConf = new SparkConf().setAppName("suffix tree validation")
    //    sparkConf.setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val sql = new SQLContext(sc)
    import sql.implicits._

    def wholeTextFile2DF(path: String, partition: Int, columnName: String) = {
      sc.wholeTextFiles(path, partition).flatMap { case (_, content) =>
        wholeFileOp(content)
      }.partitionBy(new HashPartitioner(partition)).toDF(Seq("name", columnName): _*)
    }
    def textFile2DF(path: String, partition: Int, columnName: String) = {
      sc.textFile(path).coalesce(partition, false).map(lineOp(_)).filter(_ != null)
        .partitionBy(new HashPartitioner(partition)).toDF(Seq("name", columnName): _*)
    }

    val df1 = if (op.toLowerCase == "whole")
      wholeTextFile2DF(correctPath, partition, "correctRes")
    else
      textFile2DF(correctPath, partition, "correctRes")

    val df2 = if (op.toLowerCase == "whole")
      wholeTextFile2DF(judgePath, partition, "judgeRes")
    else
      textFile2DF(judgePath, partition, "judgeRes")

    val countCorrect = df1.count()
    val countJudge = df2.count()
    val linesFlag = if (countCorrect != countJudge) "NOT" else ""

    println("\n\nCorrect Count is  " + countCorrect + "\nJudge Count is    " + countJudge + "\nTotal Number is " + linesFlag + " equal")

    val df3 = df1.join(df2, Seq("name")).cache

    df3.registerTempTable("dd")

    //println(df1.count)
    //println(df2.count)
    println("\n\nJoin Count is " + df3.count())

    val res = sql.sql("SELECT * FROM dd WHERE judgeRes != correctRes").cache
    val count = res.count()
    println("\n\nDifferent number is " + count)
    res.show()
  }

  def wholeFileOp(content: String) = {
    val x = content.split("\n")
    val res = x.map { item =>
      val pair = item.split(" ")
      if (pair.size == 2)
        (pair(1), pair(0))
      else null
    }.filter(_ != null)
    res
  }

  def lineOp(x: String) = {
    val pair = x.split(" ")
    if (pair.size == 2)
      (pair(1), pair(0))
    else
      null
  }
}
