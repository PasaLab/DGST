package util.sorting.system

import java.util.Comparator

import gct.rdd.material.io.SharedBufferSortableArray
import gct.rdd.smalltree.SALcpForBuild

object JavaSort {

  def apply(input: SharedBufferSortableArray): Unit = {
    val start = input.start()
    val end = input.end() + 1
    apply(input, start, end)
  }

  def apply(input: SharedBufferSortableArray, start: Int, end: Int): Unit = {
    java.util.Arrays.sort(input.sortingArray, start, end, new SALCPComparator(input))
  }

  class SALCPComparator(input: SharedBufferSortableArray) extends Comparator[SALcpForBuild] {
    val x = input

    def compare(o1: SALcpForBuild, o2: SALcpForBuild): Int = {
      val start1 = input.getBufferStart(o1)
      val start2 = input.getBufferStart(o2)
      if (start1 < 0 || start2 < 0)
        throw new Exception("suffix2bufferIndex < 0")

      -input.lessThanWithoutIndex(start1, start2, 0)
    }
  }

}
