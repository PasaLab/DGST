package gct.rdd

import gct.rdd.divide.ERaStrategy
import gct.rdd.group.{ERaPartitioner, Grouping}
import gct.rdd.input.MergeDirToFile
import gct.rdd.material.ERLcpWithFS
import gct.rdd.material.lcp.base.Location
import gct.rdd.smalltree.{LcpMergeSubTree, SALcpForBuild}
import org.apache.spark.rdd.RDD
import org.eclipse.collections.api.map.MutableMap
import org.eclipse.collections.impl.list.mutable.FastList
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import util.matching.TrieGraph

import scala.collection.JavaConverters._

/**
 * ERa algorithm
 */
class EraTree(config: GctConf) extends Serializable {

  val conf = config
  val div = new ERaStrategy
  var subTrees: RDD[(String, LcpMergeSubTree)] = _
  var roots: FastList[(String, Long)] = _

  def build(input: RDD[(Array[Byte], String)]): Unit = {

    var startTime = System.nanoTime()
    var endTime = System.nanoTime()

    val sc = input.context


    startTime = System.nanoTime()
    val fileName2RangeMapper = MergeDirToFile(conf.inputDir, conf)
    endTime = System.nanoTime()
    println("Merge to File, time:" + ((endTime - startTime) / 1000000) + "ms")

    startTime = System.nanoTime()
    //Sub-tree Partitioning
    roots = divide(conf.maxCount)

    endTime = System.nanoTime()
    println("divide, time:" + ((endTime - startTime) / 1000000) + "ms")
    println("roots num:" + roots.size() + " sum :" + (for (i <- 0 until roots.size()) yield roots.get(i)._2).sum)

    val bcPrefixDepth = input.context.broadcast(div.mainTrie.print(conf.outputLocation + conf.dirSpliter + "MainTrieRes"))
    startTime = System.nanoTime()
    //First Best Task allocation strategy
    val groupingMap = grouping(roots, conf.maxCount)
    endTime = System.nanoTime()
    println("groupingMap, time:" + ((endTime - startTime) / 1000000) + "ms")
    println("Grouping Num: " + groupingMap.values().asScala.max + " size:" + groupingMap.values().size())

    val iters = makeIter(groupingMap, conf.partition)
    //Review the load balance
    Grouping.review(roots, groupingMap, conf.nodeBudget, conf.partition)

    var iterNum = 0
    iters.foreach { currentRoots =>
      iterNum += 1
      println("======================================")
      println("Building group " + iterNum + "start")
      println("======================================")
      val unitStart = System.nanoTime()
      startTime = System.nanoTime()
      val bcRoots = sc.broadcast(currentRoots)
      endTime = System.nanoTime()
      println("broadcast, time:" + ((endTime - startTime) / 1000000) + "ms")

      startTime = System.nanoTime()
      val locRDD = sc.parallelize(currentRoots.asScala.zipWithIndex).partitionBy(new ERaPartitioner(conf.partition, groupingMap)).mapPartitions { x =>
        val prefixes = x.map(_._1.getBytes)
        val dx = findLocs(prefixes.toArray, conf.alphabetNum, fileName2RangeMapper)
        dx.toIterator
      }
      endTime = System.nanoTime()
      println("Find Loc, time:" + ((endTime - startTime) / 1000000) + "ms")

      startTime = System.nanoTime()

      //Construct ordered suffix array by ERa sorting
      val subTreeMaterial = locRDD.mapPartitions(ersort(_, conf.range, fileName2RangeMapper).toIterator)
      endTime = System.nanoTime()
      println("subTreeMaterial, time:" + ((endTime - startTime) / 1000000) + "ms")

      startTime = System.nanoTime()
      subTreeMaterial.foreach { case (prefix, material) =>
        //Build sub-tree according to the suffix array in linear time
        val tree = buildSubTree(prefix.getBytes, material)
        val prefixDepth = bcPrefixDepth.value.toMap.get(prefix).get
        tree.printAllSuffix(prefixDepth, conf)
      }
      endTime = System.nanoTime()
      println("subTree, time:" + ((endTime - startTime) / 1000000) + "ms")

      val unitEnd = System.nanoTime()
      println("Building group " + iterNum + "complete. time: " + ((unitEnd - unitStart) / 1000000) + "ms")
    }
  }

  def divide(budget: Int): FastList[(String, Long)] = {
    div.divide(conf, budget)
  }

  def grouping(input: FastList[(String, Long)], budget: Int): MutableMap[String, Int] = {
    Grouping.firstFitDesc(input, budget)
  }

  def makeIter(groupingMap: MutableMap[String, Int], partitions: Int): Array[FastList[String]] = {
    Grouping.makeIter(groupingMap, partitions)
  }

  def findLocs(prefixes: Array[Array[Byte]], alphabetNum: Int,
               fileName2StartIndMapper: UnifiedMap[String, (Long, Long)]): Array[(String, FastList[Location])] = {
    TrieGraph.findAllLocs(prefixes, alphabetNum, fileName2StartIndMapper, conf)
  }

  def ersort(input: Iterator[(String, FastList[Location])], range: Int,
             fileName2StartMap: UnifiedMap[String, (Long, Long)]): Array[(String, Array[SALcpForBuild])] = {

    ERLcpWithFS.ersort(input, fileName2StartMap, conf)
  }

  def buildSubTree(pattern: Array[Byte], material: Array[SALcpForBuild]): LcpMergeSubTree = {
    val root = new LcpMergeSubTree(pattern)
    root.build(material)
    root
  }
}
