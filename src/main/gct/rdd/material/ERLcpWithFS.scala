package gct.rdd.material

import gct.rdd.GctConf
import gct.rdd.material.io.{ReadDataFromMergedFile, SharedBufferSortableArray, SortingGroupInfo}
import gct.rdd.material.lcp.LcpMerge
import gct.rdd.material.lcp.base.{Location, Player}
import gct.rdd.smalltree.SALcpForBuild
import org.eclipse.collections.impl.factory.BiMaps
import org.eclipse.collections.impl.list.mutable.FastList
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import util.sorting.Sorting
import util.sorting.quick.NaiveQuickSort

import scala.collection.Iterator
import scala.collection.mutable.ListBuffer

/**
 * LCP-Merge sorting and Elastic range sorting
 */
object ERLcpWithFS {

  def apply(input: scala.Iterator[(String, scala.Iterable[Iterator[Player]])],
            fileName2StartMap: UnifiedMap[String, (Long, Long)], conf: GctConf): Array[(String, Array[SALcpForBuild])] = {

    var start = System.nanoTime()

    //Multi-way LCP-Merge sorting
    val res = input.map { case (prefix, mergingMaterial) =>
      (prefix, firstMerge(mergingMaterial.toArray, conf))
    }.toArray


    var end = System.nanoTime()
    println("firstMerge complete, time:" + ((end - start) / 1000000) + "ms")
    start = System.nanoTime()
    //Elastic range sorting for out-of-order intervals
    getInputBufferAndERSort(res, fileName2StartMap, conf)
    end = System.nanoTime()
    println("ersort iteration complete, time:" + ((end - start) / 1000000) + "ms")
    res
  }

  /**
   * LCP-Merge sorting used in DGST algorithm
   */
  def firstMerge(input: Array[Iterator[Player]], conf: GctConf): Array[SALcpForBuild] = {
    LcpMerge(input, conf)
  }

  /**
   * Elastic range sorting used in ERa algorithm
   */
  def ersort(input: Iterator[(String, FastList[Location])],
             fileName2StartMap: UnifiedMap[String, (Long, Long)],
             conf: GctConf): Array[(String, Array[SALcpForBuild])] = {
    val allLocsGroupedByPrefix = input.toArray
    val res = new Array[(String, Array[SALcpForBuild])](allLocsGroupedByPrefix.size)

    var ind = 0

    //Initialize each element of Suffix array
    while (ind < allLocsGroupedByPrefix.length) {
      val tmp = allLocsGroupedByPrefix(ind)
      res(ind) = (tmp._1, new Array[SALcpForBuild](tmp._2.size()))
      var i = 0
      while (i < tmp._2.size()) {
        res(ind)._2(i) = new SALcpForBuild(tmp._2.get(i), (0, 0, -1))
        i += 1
      }
      ind += 1
    }

    val start = System.nanoTime()
    getInputBufferAndERSort(res, fileName2StartMap, conf)
    val end = System.nanoTime()
    println("ersort complete, time:" + ((end - start) / 1000000) + "ms")

    res
  }

  /**
   * Find the unsorted intervals, read the merged string and then sort the out-of-order intervals
   */
  def getInputBufferAndERSort(res: Array[(String, Array[SALcpForBuild])],
                              fileName2StartIndMapper: UnifiedMap[String, (Long, Long)],
                              conf: GctConf): Array[(String, Array[SALcpForBuild])] = {
    val prefix2IndexBiMap = BiMaps.mutable.empty[String, Int]()
    val prefixes = res.map(_._1)
    var i = 0
    while (i < prefixes.size) {
      prefix2IndexBiMap.put(prefixes(i), i)
      i += 1
    }
    var remainGroups = res.flatMap { case (prefix, salcp) =>
      findUnsortRange(salcp, 0, salcp.size - 1, prefix2IndexBiMap.get(prefix), conf.segmentLen)
    }

    val buffer = new Array[Byte](conf.sortingBufferSize)

    // todo start from lcpRange
    var itersum = 0
    while (remainGroups.size > 0) {
      itersum += 1
      //save first lcp
      val firstLcps = remainGroups.map { case SortingGroupInfo(prefixInd, startInd, endInd, offset) =>
        res(prefixInd)._2(startInd).lcp
      }
      val targetsOrgnizedByIO = new scala.collection.mutable.HashMap[String, FastList[Long]]
      var unsortNum = 0
      remainGroups.foreach { case SortingGroupInfo(prefixInd, startInd, endInd, offset) =>
        val currentRes = res(prefixInd)
        var i = startInd
        while (i <= endInd) {
          val salcp = currentRes._2(i)
          targetsOrgnizedByIO.get(salcp.location.fileName) match {
            case Some(x) => {
              x.add(salcp.getOffset(offset))
            }
            case none => {
              targetsOrgnizedByIO.put(salcp.location.fileName, FastList.newListWith(salcp.getOffset(offset)))
            }
          }
          unsortNum += 1
          i += 1
        }
      }
      val bufferSizeForEachSuffix = buffer.size / unsortNum

      val locs2BufferStartMapper = ReadDataFromMergedFile(
        targetsOrgnizedByIO,
        buffer, bufferSizeForEachSuffix,
        fileName2StartIndMapper,
        conf.fsReadBufferSize: Int, conf.sortingBufferSize: Int,
        conf.mergedFilePath: String
      )

      remainGroups.foreach { group =>
        val sortableArray = new SharedBufferSortableArray(
          buffer, group, locs2BufferStartMapper,
          res(group.prefixInd)._2, bufferSizeForEachSuffix, group.lcpStart
        )
        if (group.endInd - group.startInd < 5)
          NaiveQuickSort(sortableArray, false)
        else
          Sorting(sortableArray, conf.sortingMethod)
      }

      val newGroupInfo = new ListBuffer[SortingGroupInfo]

      def makeLcp(smaller: SALcpForBuild, larger: SALcpForBuild, offset: Int) = {
        var res: (Byte, Byte, Int) = (0, 0, -1)
        var i = 0
        val start1 = locs2BufferStartMapper.get((smaller.location.fileName, smaller.getOffset(offset)))
        val start2 = locs2BufferStartMapper.get((larger.location.fileName, larger.getOffset(offset)))
        while (i < bufferSizeForEachSuffix && res != null && res._3 == -1) {
          if (buffer(start1 + i) != buffer(start2 + i))
            res = (buffer(start1 + i), buffer(start2 + i), offset + i)

          //totally equal
          else if (buffer(start1 + i) < 0)
            res = null

          i += 1
        }
        res
      }
      //make lcp
      i = 0
      while (i < remainGroups.size) {
        val info = remainGroups(i)
        res(info.prefixInd)._2(info.startInd).lcp = firstLcps(i)
        i += 1
      }

      remainGroups.foreach { case SortingGroupInfo(prefixInd, startInd, endInd, offset) =>
        val x = res(prefixInd)._2
        var suffixInd = startInd + 1
        while (suffixInd <= endInd) {
          x(suffixInd).lcp = makeLcp(x(suffixInd - 1), x(suffixInd), offset)
          suffixInd += 1
        }

        // special case for the annoying ERROR "lcp disorder former = latter"
        suffixInd = startInd + 1
        if (x(startInd).lcp != null && x(suffixInd).lcp != null &&
          x(startInd).lcp._3 == x(suffixInd).lcp._3 && x(startInd).lcp._2 == x(suffixInd).lcp._2) {
          if (x(startInd).lcp._1 < x(suffixInd).lcp._1) {
            throw new Exception("mysterious ERROR happened")
          }
        }
        // specially for era
        if (startInd == 0 && endInd >= 1 && conf.disableSegment == true) {
          if (itersum == 1) {
            val tmp = locs2BufferStartMapper.get((x(0).location.fileName, x(0).getOffset(offset)))
            x(0).lcp = (buffer(tmp), 0, -1)
          }
          var second = 1
          while (second < x.size && x(second).lcp == null)
            second += 1
          if (second < x.size && x(second).lcp != null && x(second).lcp._3 > -1)
            x(0).lcp = (x(0).lcp._1, x(second).lcp._1, 0)
        }
        newGroupInfo ++= findUnsortRange(x, startInd, endInd, prefixInd, bufferSizeForEachSuffix + offset)
      }
      remainGroups = newGroupInfo.toArray
    }
    res
  }

  /**
   * Find unsorted intervals in suffix array
   */
  def findUnsortRange(input: Array[SALcpForBuild], startInd: Int, endInd: Int,
                      prefixInd: Int, comparedOffset: Int): Array[SortingGroupInfo] = {
    val res = new ListBuffer[SortingGroupInfo]

    var suffixInd = startInd + 1
    while (suffixInd <= endInd) {
      var i = suffixInd
      if (input(i).lcp != null && input(i).lcp._3 < 0) {

        // here -2 to ensure correctness safety
        val preLcp = comparedOffset

        var preLen = i - 1
        while (preLen > 0 && (input(preLen).lcp == null || input(preLen).lcp._3 < 0)) {
          preLen -= 1
        }

        while (i <= endInd && (input(i).lcp == null || (input(i).lcp._3 < 0))) {
          i += 1
        }

        val end = i - 1
        res += new SortingGroupInfo(prefixInd, preLen, end, preLcp)
        if (preLcp < 0) throw new Exception("prelcp < 0 :" + preLcp)
        suffixInd = i
      }
      else
        suffixInd += 1
    }
    res.toArray
  }

}
