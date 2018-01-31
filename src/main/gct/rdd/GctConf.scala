package gct.rdd

import java.io.File

import com.typesafe.config.ConfigFactory

/**
 * Configuration
 */
class GctConf extends Serializable {

  val x = System.getProperty("spark.driver.extraClassPath")
  val conf = if (x == null) {
    ConfigFactory.parseResources("conf.properties")
  }
  else
    ConfigFactory.parseFile(new File(x + "/conf.properties"))
  //========== OS ===============
  val dirSpliter = System.getProperty("os.name").toLowerCase.take(7) match {
    case "windows" => '\\'
    case _ => '/'
  }
  var disableSegment = true
  //========= alphabet ==========
  var alphabetNum: Int = conf.getInt("alphabet.num")
  //============ File ===========
  var fsExtraLen: Int = conf.getInt("fs.extra.len")
  // ========= dividing =========
  var startLen: Int = conf.getInt("div.start")
  var step: Double = conf.getDouble("div.step")
  //========= budgets ===============
  var maxCount: Int = conf.getInt("root.max.count")
  var budgetFactor: Int = conf.getInt("budget.factor")
  //=========== lcp compare ============
  var unknownElement: Byte = conf.getInt("unknown.element").toByte
  var firstBuf: Int = conf.getInt("first.buffer")
  var range: Int = conf.getInt("lcp.range")
  //=========== ERa sorting ================
  var fsReadBufferSize: Int = memParse(conf.getString("fs.read.buffer.size"))
  var sortingBufferSize: Int = memParse(conf.getString("sorting.buffer.size"))
  //=============   SORTING  =================
  var sortingMethod = conf.getString("sorting.method")
  var sortingCutOff = conf.getInt("sorting.cutoff")
  //============ Spark properties ===============
  var partition: Int = conf.getInt("spark.partitions")
  //============== grouping ==================
  var groupingMethod = conf.getString("grouping.method")
  //================= INPUT&OUTPUT ===========
  var workingDir: String = conf.getString("working.dir")
  var inputDir = conf.getString("input.dir")
  var localInputDir = ""
  var outputLocation = conf.getString("output.location")
  var MergedFileName: String = conf.getString("merged.filename")
  var fsWriteBufferSize: Int = memParse(conf.getString("fs.write.buffer.size"))

  def segmentLen: Int = if (disableSegment) 0 else 32 / bitsPerItem

  def bitsPerItem: Int = math.round(math.log(alphabetNum + 1) / math.log(2) + 0.5).toInt

  def getFileName(fileName: String) = fileName.substring(fileName.lastIndexOf(dirSpliter) + 1)

  //这个budget.factor是干嘛的
  def nodeBudget: Int = maxCount * budgetFactor

  def mergedFilePath: String = workingDir + dirSpliter + MergedFileName

  def memParse(input: String): Int = {
    val factor = input.last.toLower match {
      case 'k' => 1024
      case 'm' => 1024 * 1024
      case 'g' => 1024 * 1024 * 1024
      case _ => 1
    }
    Integer.parseInt(input.substring(0, input.size - 1)) * factor
  }

  def getOutputFile(fileName: String) = outputLocation + dirSpliter + fileName
}
