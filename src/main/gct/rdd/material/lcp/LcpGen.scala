package gct.rdd.material.lcp

import gct.rdd.GctConf
import gct.rdd.divide.SegmentBasedPrefixInfo
import gct.rdd.material.lcp.base.{Location, Player, PlayerMaker, SharedBufferSuffixArray}

import scala.collection.JavaConverters._

/**
 * Generate local LCP-Range array for each input split
 */
object LcpGen {

  def apply(prefixInfo: SegmentBasedPrefixInfo, conf: GctConf) = {

    val locs = prefixInfo.loc.asScala.toArray
    val sa = new SharedBufferSuffixArray(locs, true)
    val res = new Array[Player](sa.num)

    res(0) = new Player(new Location(sa(0).location3, prefixInfo.len), 0,
      sa(0).getSegment(conf.bitsPerItem, prefixInfo.len))
    res(0).content = sa(0).toArrayAndTake(prefixInfo.len, conf.firstBuf)
    res(0).lcp = (0, Array[Byte](0), 0)
    res(0).existRemainItem = if (sa(0).length - prefixInfo.len <= math.max(conf.segmentLen, conf.firstBuf)) false else true
    var i = 1
    var lcpMax = 0
    while (i < res.size) {
      val segment = sa(i).getSegment(conf.bitsPerItem, prefixInfo.len)
      val lcp = sa.lcpForBuild(i, conf.range, prefixInfo.len)
      lcpMax = math.max(lcpMax, lcp._3)
      val loc = new Location(sa(i).location3, prefixInfo.len)

      val remain = if (lcp == null)
        false
      else {
        /**
         * note that lcp._2.length ==0 && lcp._3 == 5 => suffixLength = 5
         * lcp._2.length ==2 && lcp._3 == 2 => suffixLength = 3
         */
        val LcpExtraLength = if (lcp._2.length == 0) 0 else lcp._2.length - 1

        //suffix has no more elements left
        if (LcpExtraLength + lcp._3 >= sa(i).length - prefixInfo.len)
          false
        else if (sa(i).length - prefixInfo.len <= conf.segmentLen)
          false
        else
          true
      }
      //Make player for each suffix
      res(i) = PlayerMaker.makePlayer(loc, 0, segment, lcp, remain)
      i += 1
    }
    res.toIterator
  }

}
