package gct.rdd.divide

import gct.rdd.GctConf
import gct.rdd.input.InputSplit
import gct.rdd.top.trie.TopTrie
import org.apache.spark.rdd.RDD
import org.eclipse.collections.impl.list.mutable.FastList

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Divide strategy used in DGST algorithm
 * @param conf
 */
class InMemMergeFileStrategy(conf: GctConf) extends DivideStrategy {
  //Global frequency trie
  val mainTrie = new TopTrie
  var beginLength = conf.startLen
  var alpha = conf.step

  /**
   * Count the frequency of S-prefix in parallel according to the input split
   * @param input: InputSplit RDD
   * @param budget: Max Frequency
   * @return
   */
  def divide(input: RDD[InputSplit], budget: Int): FastList[(String, Long)] = {

    val sc = input.context

    //Build local frequency trie for each input split
    var inputAndTrie = input.map((_, new TopTrie))

    val remainTargets = FastList.newList[String]
    val terminalInfo = new ListBuffer[(String, (FastList[(String, Long)]))]
    val roots = FastList.newList[(String, Long)]

    var currentLength = beginLength
    var lastTargetLength = 0
    var iterNum = 0
    do {
      iterNum += 1
      println(s"Iter $iterNum start, currentLength = $currentLength")

      val bcRemainTargets = sc.broadcast(remainTargets)
      var startTime = System.nanoTime()
      inputAndTrie = inputAndTrie.map { case (split, trie) =>
        countPrefix(split, trie, currentLength, lastTargetLength, bcRemainTargets.value)
        (split, trie)
      }
      var endTime = System.nanoTime()
      println("counting: " + (endTime - startTime) / 1000000 + "ms")

      startTime = System.nanoTime()
      val collectInfo = inputAndTrie.flatMap { case (_, trie) =>
        trie.getCollectInfo().asScala
      }.reduceByKey((x, y) => x + y).collect()
      endTime = System.nanoTime()
      println("collect: " + (endTime - startTime) / 1000000 + "ms")

      startTime = System.nanoTime()
      terminalInfo ++= collectInfo.filter(_._2.terminalLoc != null).map(x => (x._1, x._2.terminalLoc))
      println("Terminal Num for length " + currentLength + ":" + terminalInfo.size)
      val prefixCounts = collectInfo.filter(_._1.length == currentLength).map(x => (x._1, x._2.count))
      println("prefix Num for length " + currentLength + ":" + prefixCounts.map(_._2).sum)

      remainTargets.clear()
      prefixCounts.foreach { case (prefix, count) =>
        if (count > budget)
          remainTargets.add(prefix)
        else
          roots.add((prefix, count))
      }
      endTime = System.nanoTime()
      println("filter: " + (endTime - startTime) / 1000000 + "ms")

      lastTargetLength = currentLength
      currentLength = (currentLength + alpha).toInt
    } while (remainTargets.size > 0)

    mainTrie.buildWithFinalResult(roots, terminalInfo.toArray, budget)
    mainTrie.getAllRoots()
  }

  /**
   * Count the frequency of S-prefix in each input split
   * @param split: input split
   * @param trie: frequency trie
   * @param currentLength: current count window
   * @param lastTargetLength: last count window
   * @param remainTargets
   */
  def countPrefix(split: InputSplit, trie: TopTrie, currentLength: Int, lastTargetLength: Int, remainTargets: FastList[String]): Unit = {
    trie.tag(remainTargets)
    var i = 0
    var fileSegmentStartIndex = 0

    while (i < split.segments.length - 1 && split.segments(i).len < lastTargetLength) i += 1

    //Traverse each segment in split. Different segments indicate different strings.
    while (i < split.segments.length) {
      val currentFileSegment = split.segments(i)

      var inFileIndex = 0
      while (inFileIndex + currentLength < currentFileSegment.len) {
        trie.addWithSplit(currentFileSegment.fileName, split.content,
          inFileIndex + fileSegmentStartIndex, inFileIndex + fileSegmentStartIndex + currentLength,
          currentLength, lastTargetLength, currentFileSegment.start - fileSegmentStartIndex)
        inFileIndex += 1
      }
      while (currentFileSegment.remainNum == 0 && inFileIndex + lastTargetLength < currentFileSegment.len) {
        trie.addWithSplit(currentFileSegment.fileName, split.content,
          inFileIndex + fileSegmentStartIndex, fileSegmentStartIndex + currentFileSegment.len,
          currentLength, lastTargetLength, currentFileSegment.start - fileSegmentStartIndex, true)
        inFileIndex += 1
      }

      if (currentFileSegment.remainNum > 0) {
        while (inFileIndex < currentFileSegment.len && inFileIndex + currentLength < currentFileSegment.len + split.extraLen) {
          trie.addWithSplit(currentFileSegment.fileName, split.content,
            inFileIndex + fileSegmentStartIndex, inFileIndex + fileSegmentStartIndex + currentLength,
            currentLength, lastTargetLength, currentFileSegment.start - fileSegmentStartIndex)
          inFileIndex += 1
        }
        while (currentFileSegment.remainNum < currentLength && inFileIndex + lastTargetLength < currentFileSegment.len + currentFileSegment.remainNum) {
          trie.addWithSplit(currentFileSegment.fileName, split.content,
            inFileIndex + fileSegmentStartIndex, (fileSegmentStartIndex + currentFileSegment.len + currentFileSegment.remainNum).toInt /*因为while中的条件表明是remainNum<currentLength*/ ,
            currentLength, lastTargetLength, currentFileSegment.start - fileSegmentStartIndex, true)
          inFileIndex += 1
        }
      }
      fileSegmentStartIndex += currentFileSegment.len
      i += 1
    }
  }
}
