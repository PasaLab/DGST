package gct.rdd.material.lcp.base

import java.util.Comparator

/**
 * Suffix comparator
 */
class SharedBufferSuffixComparator extends Comparator[SharedBufferSuffix] {
  def compare(o1: SharedBufferSuffix, o2: SharedBufferSuffix): Int = o1.compare(o2)
}
