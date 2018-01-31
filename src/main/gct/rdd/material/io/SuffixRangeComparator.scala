package gct.rdd.material.io

import java.util.Comparator

/**
 * Suffix range comparator
 */
class SuffixRangeComparator extends Comparator[(Long, Long)] {
  override def compare(o1: (Long, Long), o2: (Long, Long)): Int = {
    if (o1._1 < 0 || o2._1 < 0)
      throw new Exception("location2fileIndex < 0")

    if (o1._1 - o2._1 > 0) return 1
    else if (o1._1 - o2._1 < 0) return -1;
    else {
      return 0
    }
  }
}
