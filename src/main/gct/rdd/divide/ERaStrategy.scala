package gct.rdd.divide

import gct.rdd.GctConf
import gct.rdd.top.trie.TopTrie
import org.eclipse.collections.impl.list.mutable.FastList
import util.file.{FSReaderFactory, InputStatus, ListFiles}

/**
 * Divide strategy used in ERa algorithm
 */
class ERaStrategy extends DivideStrategy {

  val mainTrie = new TopTrie

  /**
   * Access all files sequentially and calculate the total frequency of S-prefix
   * @param conf
   * @param budget: Max Frequency
   * @return
   */
  def divide(conf: GctConf, budget: Int): FastList[(String, Long)] = {

    val input = ListFiles(conf.inputDir)

    var startTime = System.nanoTime()
    var length = 1
    do {
      println(s"iter $length start")
      input.foreach { fileName =>
        countPrefix(fileName, length, conf)
      }
      var endTime = System.nanoTime()
      println("length: " + length + " :time: " + (endTime - startTime) / 1000000 + "ms")
      length += 1
    } while (mainTrie.shouldProceed(budget))

    mainTrie.getAllRoots()
  }

  /**
   * Count the frequency of S-prefix
   * @param fileName
   * @param length: The length of S-prefix
   * @param conf
   */
  def countPrefix(fileName: String, length: Int, conf: GctConf): Unit = {

    val fileLen = InputStatus.getLength(fileName)
    val actualFileName = conf.getFileName(fileName)

    val reader = FSReaderFactory(fileName)

    val crossBuffer = new Array[Byte](2 * length)

    var haveReadNum: Long = 0
    var crossBufferFlag = false
    val bufferSize = math.min(fileLen, conf.fsReadBufferSize).toInt
    val buffer = new Array[Byte](bufferSize)
    var start = System.nanoTime()
    var readSize = reader.read(buffer)
    while (readSize > 0) {

      start = System.nanoTime()
      if (crossBufferFlag) {
        val crossNum = math.min(readSize, length)
        System.arraycopy(buffer, 0, crossBuffer, length, crossNum)
        var ind = 0
        while (ind <= crossNum) {
          val flag = if (ind == crossNum && length >= readSize && readSize <= crossNum) true else false
          mainTrie.addOnlyLeaf(actualFileName, crossBuffer, ind, ind + length, haveReadNum - length, flag)
          ind += 1
        }
      }

      var ind = 0
      while (ind + length <= readSize) {
        val flag = if (ind + length == readSize && haveReadNum + ind + length >= fileLen) true else false
        mainTrie.addOnlyLeaf(actualFileName, buffer, ind, ind + length, haveReadNum, flag)
        ind += 1
      }
      haveReadNum += readSize
      crossBufferFlag = true
      start = System.nanoTime()
      readSize = reader.read(buffer)
    }
  }
}
