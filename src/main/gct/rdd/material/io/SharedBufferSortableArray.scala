package gct.rdd.material.io

import gct.rdd.smalltree.SALcpForBuild
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import util.sorting.SortableArray

class SharedBufferSortableArray(buf: Array[Byte], info: SortingGroupInfo, mapper: UnifiedMap[(String, Long), Int],
                                input: Array[SALcpForBuild], size: Int, offset: Int) extends SortableArray {

  val buffer = buf
  val bufferComparedOffset = offset
  val sortingGroupInfo = info
  val bufferSizeForEachSuffix = size
  val locs2BufferStartMapper = mapper
  var sortingArray = input

  override def start(): Int = sortingGroupInfo.startInd

  override def end(): Int = sortingGroupInfo.endInd

  def swap(ind1: Int, ind2: Int) = {
    val tmp = sortingArray(ind1)
    sortingArray(ind1) = sortingArray(ind2)
    sortingArray(ind2) = tmp
  }

  def getBufferStart(salcp: SALcpForBuild): Int = {
    locs2BufferStartMapper.get((salcp.location.fileName, salcp.getOffset(bufferComparedOffset)))
  }

  def lessThanWithoutIndex(start1: Int, start2: Int, prefixLen: Int): Int = {
    //prefix too long then return equal
    var res = 0
    var i = prefixLen
    while (i < bufferSizeForEachSuffix && res == 0) {
      if (buffer(start1 + i) < buffer(start2 + i) && buffer(start1 + i) >= 0)
        res = 1
      else if (buffer(start1 + i) > buffer(start2 + i) && buffer(start2 + i) >= 0)
        res = -1
      else if (buffer(start2 + i) >= 0 && buffer(start1 + i) < 0)
        res = -1
      else if (buffer(start1 + i) >= 0 && buffer(start2 + i) < 0)
        res = 1
      i += 1
    }
    res
  }

  def lessThan(x1: Int, x2: Int) = {
    lessThan(x1, x2, 0)
  }

  def lessThan(x1: Int, x2: Int, prefixLen: Int): Int = {
    val start1 = getBufferStart(x1)
    val start2 = getBufferStart(x2)
    //prefix too long then return equal
    var res = 0
    var i = prefixLen
    while (i < bufferSizeForEachSuffix && res == 0) {
      if (buffer(start1 + i) < buffer(start2 + i) && buffer(start1 + i) >= 0)
        res = 1
      else if (buffer(start1 + i) > buffer(start2 + i) && buffer(start2 + i) >= 0)
        res = -1
      else if (buffer(start2 + i) >= 0 && buffer(start1 + i) < 0)
        res = -1
      else if (buffer(start1 + i) >= 0 && buffer(start2 + i) < 0)
        res = 1
      i += 1
    }
    res

  }

  def getBufferStart(i: Int): Int = {
    val salcp = apply(i)
    locs2BufferStartMapper.get((salcp.location.fileName, salcp.getOffset(bufferComparedOffset)))
  }

  def apply(i: Int) = sortingArray(i)

  override def posLessThan(x1: Int, x2: Int, prefixLen: Int): Int = {
    val start1 = getBufferStart(x1) + prefixLen
    val start2 = getBufferStart(x2) + prefixLen
    var res = 0

    if (buffer(start1) < buffer(start2) && buffer(start1) >= 0)
      res = 1
    else if (buffer(start1) > buffer(start2) && buffer(start2) >= 0)
      res = -1
    else if (buffer(start2) >= 0 && buffer(start1) < 0)
      res = -1
    else if (buffer(start1) >= 0 && buffer(start2) < 0)
      res = 1
    res
  }

  override def prefixTooLong(prefixLen: Int): Boolean = return prefixLen > bufferSizeForEachSuffix
}
