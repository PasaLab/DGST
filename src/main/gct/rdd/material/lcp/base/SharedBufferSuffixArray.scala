package gct.rdd.material.lcp.base

import org.apache.log4j.LogManager


/**
 * Suffix array
 */
class SharedBufferSuffixArray(input: Array[SharedBufferSuffix], needSort: Boolean = false) extends Serializable {
  val log = LogManager.getRootLogger
  val suffixes = input

  //todo 3-way radix sort
  if (needSort)
    java.util.Arrays.sort(suffixes, new SharedBufferSuffixComparator)

  val num: Int = input.length

  /**
   * Returns the location of the ith smallest suffix.
   */
  def location3(i: Int): (String, Long, Long) = {
    if (i < 0 || i >= suffixes.length) throw new IndexOutOfBoundsException
    return suffixes(i).location3
  }

  def location2(i: Int): (String, Long) = {
    if (i < 0 || i >= suffixes.length) throw new IndexOutOfBoundsException
    return suffixes(i).location2
  }

  /**
   * Returns the length of the longest common prefix of the ith
   * smallest suffix and the i-1st smallest suffix.
   */
  def lcp(i: Int): Int = {
    if (i < 1 || i >= suffixes.length) throw new IndexOutOfBoundsException
    return lcp(suffixes(i), suffixes(i - 1))
  }

  /**
   * Calculate the length of the longest common prefix between two suffixes
   */
  def lcp(s: SharedBufferSuffix, t: SharedBufferSuffix): Int = {
    var n: Long = Math.min(s.length, t.length)
    var i: Int = 0
    try {
      if (n > Integer.MAX_VALUE) {
        n = Integer.MAX_VALUE
      }
      while (i < n) {
        if (s.index(i) != t.index(i)) return i
        i += 1
      }

      if (n < Integer.MAX_VALUE) return n.toInt
      else {
        println("oops! bigger than integer.maxvalue!")
        return Integer.MAX_VALUE
      }

    }
    catch {
      case _ => {
        // unfinished suffix
        return -i
      }
    }
  }

  def lcpForBuild(i: Int, lcpLen: Int, prefixLen: Int): (Byte, Array[Byte], Int) = {
    val res = lcpForBuild(i, lcpLen)
    if (res == null)
      return null
    else
      return (res._1, res._2, res._3 - prefixLen)
  }

  /**
   * Build lcp information for suffix(i) and suffix(i-1)
   */
  def lcpForBuild(i: Int, lcpLen: Int): (Byte, Array[Byte], Int) = {
    val res = lcp(suffixes(i), suffixes(i - 1))
    //unfinished
    if (res < 0) {
      return ((-1).toByte, new Array[Byte](0), res.toInt)
    }
    val len = math.min(suffixes(i).length, suffixes(i - 1).length)
    if (len > 0) {
      val lenReal = (math.min(len - res, lcpLen)).toInt
      if (suffixes(i).length == suffixes(i - 1).length && len == 0)
        return null
      val x2 = suffixes(i).copyRange(res, lenReal)

      return (suffixes(i - 1).index(res), x2, res)
    }
    else
      return (suffixes(i - 1).index(0), new Array[Byte](0), res)
  }

  def apply(i: Int): SharedBufferSuffix = {
    if (i < 0 || i >= suffixes.length) throw new IndexOutOfBoundsException
    return suffixes(i)
  }
}
