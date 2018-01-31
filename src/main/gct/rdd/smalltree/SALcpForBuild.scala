package gct.rdd.smalltree

import gct.rdd.material.lcp.base.Location

/**
 * Element of suffix array including location information and LCP informatin
 */
case class SALcpForBuild(location: Location, var lcp: (Byte, Byte, Int)) {
  def getOffset(bufferComparedOffset: Int, offset: Int): Long = getOffset(bufferComparedOffset) + offset

  def getOffset(bufferComparedOffset: Int): Long = location.startInd + location.prefixLen + bufferComparedOffset
}
