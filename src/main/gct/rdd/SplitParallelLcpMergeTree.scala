package gct.rdd

import gct.rdd.divide.{InMemMergeFileStrategy, SegmentBasedPrefixInfo}
import gct.rdd.group.{ERaPartitioner, Grouping}
import gct.rdd.input.{InputSplit, MergeDirToFile}
import gct.rdd.material.ERLcpWithFS
import gct.rdd.material.lcp.LcpGen
import gct.rdd.material.lcp.base.Player
import gct.rdd.smalltree.{LcpMergeSubTree, SALcpForBuild}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.eclipse.collections.api.map.MutableMap
import org.eclipse.collections.impl.list.mutable.FastList
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import util.matching.TrieGraph

import scala.collection.JavaConverters._

/**
 * Split-based parallel suffix tree construction algorithm
 */
class SplitParallelLcpMergeTree(config: GctConf) extends Serializable {

  val conf = config
  val div = new InMemMergeFileStrategy(conf)
  var subTreeRoots: FastList[(String, Long)] = _

  def build(sc: SparkContext): Unit = {

    var startTime = System.nanoTime()

    //Merge all input string and divide the merged string into multiple splits
    val (fileName2RangeMapper, splits) = MergeDirToFile.mergeAndSplit(conf.inputDir, conf)
    var endTime = System.nanoTime()
    println("Merge to File, time:" + ((endTime - startTime) / 1000000) + "ms")

    startTime = System.nanoTime()
    //Read content for each input split,
    val input = sc.parallelize(splits, conf.partition).map(_.readContent(conf)).cache
    endTime = System.nanoTime()
    println("repartition, time:" + ((endTime - startTime) / 1000000) + "ms")

    startTime = System.nanoTime()
    //Sub-tree partitioning
    subTreeRoots = divide(input, conf.maxCount)
    endTime = System.nanoTime()
    val bcPrefixDepth = input.context.broadcast(div.mainTrie.print(conf.outputLocation + conf.dirSpliter + "MainTrieRes"))
    println("divide, time:" + ((endTime - startTime) / 1000000) + "ms")
    println("roots num:" + subTreeRoots.size() + " sum :" + (for (i <- 0 until subTreeRoots.size()) yield subTreeRoots.get(i)._2).sum)

    startTime = System.nanoTime()
    //Sub-tree construction task allocation strategy
    val groupingMap = grouping(subTreeRoots, conf.nodeBudget)

    endTime = System.nanoTime()
    println("groupingMap, time:" + ((endTime - startTime) / 1000000) + "ms")
    println("Grouping Num: " + groupingMap.values().asScala.max + " size:" + groupingMap.values().size())

    val iters = makeIter(groupingMap, conf.partition)
    //Review the load balance
    Grouping.review(subTreeRoots, groupingMap, conf.nodeBudget, conf.partition)

    var iterNum = 0
    iters.foreach { currentRoots =>
      iterNum += 1
      println("======================================")
      println("Building group " + iterNum + "start")
      println("======================================")
      val unitStart = System.nanoTime()

      startTime = unitStart
      val bcRoots = input.context.broadcast(currentRoots)
      endTime = System.nanoTime()
      println("broadcast, time:" + ((endTime - startTime) / 1000000) + "ms")

      startTime = System.nanoTime()
      val lcpRDD = input.flatMap { input =>
        val roots = bcRoots.value
        val prefixNum = roots.size()
        println("prefixNum: " + prefixNum)
        val prefixes = roots.asScala.map(_.getBytes()).toArray
        var start = System.nanoTime()
        // Find all locs of S-prefix
        val locs = findLocs(prefixes, input)
        println("locs size " + locs.length)
        var end = System.nanoTime()
        println("trieGraph, time:" + ((end - start) / 1000000) + "ms")
        val filtered = locs.filter(x => (x.loc != null && x.loc.size() > 0))
        start = System.nanoTime()
        val res = new Array[(String, Iterator[Player])](filtered.size)
        var i = 0
        while (i < res.size) {
          val info = filtered(i)
          //Generate local lcp-range array
          res(i) = (new String(prefixes(info.ind)), LcpGen(info, conf))
          i += 1
        }
        end = System.nanoTime()
        println("single Node LcpGen, time:" + ((end - start) / 1000000) + "ms")
        res
      }
      endTime = System.nanoTime()
      println("LcpGen, time:" + ((endTime - startTime) / 1000000) + "ms")

      startTime = System.nanoTime()
      val task = lcpRDD.groupByKey(new ERaPartitioner(conf.partition, groupingMap))
      endTime = System.nanoTime()
      println("groupByKey, time:" + ((endTime - startTime) / 1000000) + "ms")

      //Construct ordered suffix array by LCP-Merge sorting and Out-of-order interval sorting
      startTime = System.nanoTime()
      val subTreeMaterial = task.mapPartitions(erlcp(_, fileName2RangeMapper).toIterator)
      endTime = System.nanoTime()
      println("subTreeMaterial, time:" + ((endTime - startTime) / 1000000) + "ms")

      startTime = System.nanoTime()
      subTreeMaterial.foreach { case (prefix, material) =>
        //Build sub-tree according to the suffix array in linear time
        val tree = buildSubTree(prefix.getBytes, material)
        val prefixDepth = bcPrefixDepth.value.toMap.get(prefix).get
        //Print the information of all leaf nodes
        tree.printAllSuffix(prefixDepth, conf)
      }
      endTime = System.nanoTime()
      println("subTree, time:" + ((endTime - startTime) / 1000000) + "ms")

      val unitEnd = System.nanoTime()
      println("Building group " + iterNum + "complete. time: " + ((unitEnd - unitStart) / 1000000) + "ms")
    }
  }

  def divide(input: RDD[InputSplit], budget: Int): FastList[(String, Long)] = {
    div.divide(input, budget)
  }

  def grouping(input: FastList[(String, Long)], budget: Int): MutableMap[String, Int] = {
    Grouping(input, budget, conf.groupingMethod, conf.partition)
  }

  def makeIter(groupingMap: MutableMap[String, Int], partitions: Int): Array[FastList[String]] = {
    Grouping.makeIter(groupingMap, partitions)
  }

  def findLocs(prefixes: Array[Array[Byte]], input: InputSplit): Array[SegmentBasedPrefixInfo] = {
    TrieGraph.findAllOccurrencesInSplit(prefixes, input, conf)
  }

  def erlcp(input: scala.Iterator[(String, scala.Iterable[Iterator[Player]])],
            fileName2StartMap: UnifiedMap[String, (Long, Long)]): Array[(String, Array[SALcpForBuild])] = {
    ERLcpWithFS(input, fileName2StartMap, conf)
  }

  def buildSubTree(pattern: Array[Byte], material: Array[SALcpForBuild]): LcpMergeSubTree = {
    val root = new LcpMergeSubTree(pattern)
    root.build(material)
    root
  }
}
