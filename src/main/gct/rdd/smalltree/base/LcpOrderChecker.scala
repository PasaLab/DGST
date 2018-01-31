package gct.rdd.smalltree.base

import gct.rdd.smalltree.SALcpForBuild
import org.eclipse.collections.impl.map.mutable.UnifiedMap

import scala.collection.JavaConverters._

/**
 * Check the lcp order in the suffix array
 */
object LcpOrderChecker {

  def checkFinalOrder(input: Array[SALcpForBuild], pattern: Array[Byte]): (Boolean, Int) = {
    val res = true
    val resStr = new StringBuilder

    resStr.append("false root ")
    pattern.map(x => resStr.append(x.toInt).append(" "))
    resStr.append("\n")
    resStr.append("num " + input.length + "\n")

    val preMap = UnifiedMap.newMap[Int, Byte]()

    var i = 1
    while (i < input.size) {
      val lcp = input(i).lcp
      if (lcp != null) {
        val len = lcp._3
        if (len < 0) {
          resStr.append("lcp_len < 0 in index " + i)
          throw new Exception(resStr.toString())
        }

        val pre = preMap.get(len)
        preMap.keySet().asScala.filter(_ > len).foreach(preMap.removeKey(_))
        if ((pre < 0 && lcp._2 > 0) || (pre > 0 && lcp._2 > 0 && pre >= lcp._2)) {
          resStr.append("lcp disorder in index " + i + '\n')
          resStr.append("former: " + pre.toInt + " latter " + lcp._2.toInt + "\n")
          throw new Exception(resStr.toString())
        }

        preMap.put(len, lcp._2)
      }
      i += 1
    }
    (res, 0)
  }

}
