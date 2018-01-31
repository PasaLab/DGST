package gct.rdd.material.lcp.base

import com.google.common.base.Objects

/**
 * Player definition of loser tree
 * @param loc                :Location of suffix
 * @param groupId           :groups
 * @param contentSegment   :segment
 * @param lcp                :lcp r.s.t former player
 * @param content            :for the first element in every group
 * @param existRemainItem   :whether remain util.item
 */
case class Player(loc: Location, var groupId: Int, val contentSegment: Int = -1,
                  var lcp: (Byte, Array[Byte], Int) = null,
                  var content: Array[Byte] = null,
                  var existRemainItem: Boolean = true) {


  def this(loc: Location, groupId: Int,
           lcp: (Byte, Array[Byte], Int),
           content: Array[Byte]) {
    this(loc, groupId, -1, lcp)
  }

  //null player
  def this(loc: Location, groupId: Int) {
    this(loc, groupId, -1)
  }

  def print() = {
    val res = Objects.toStringHelper(this)
      .add("FileName", loc.fileName)
      .add("startOffset", loc.startInd)
      .add("lcpLen", if (lcp != null && lcp._2 != null) lcp._2.length else "null")
      .add("remainItem", existRemainItem)
    if (lcp != null && lcp._2 != null && lcp._2.length > 0) {
      println(res + "   lcp:   " + new String(lcp._2.map(x => (x + '0').toByte)))
    }
  }
}

