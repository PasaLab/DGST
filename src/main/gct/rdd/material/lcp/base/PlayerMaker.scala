package gct.rdd.material.lcp.base

/**
 * Create player for loser tree
 */
object PlayerMaker {

  //Null Player is the last player used in multi-way LCP-Merge sorting
  def makeNullPlayer(groupId: Int): Player = {
    new Player(null, groupId, -1, (0, null, 0), null, false)
  }

  def makePlayer(loc: Location, groupId: Int, contentSegment: Int,
                 lcp: (Byte, Array[Byte], Int),
                 existRemainItem: Boolean): Player = {
    new Player(loc, groupId, contentSegment, lcp, null, existRemainItem)
  }

}
