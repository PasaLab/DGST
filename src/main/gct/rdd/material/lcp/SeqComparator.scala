package gct.rdd.material.lcp

import gct.rdd.material.lcp.base.Player


/**
 * Player comparator in the loser tree
 */
object SeqComparator {
  /**
   * -1: need lcp compare
   * -2: different segment
   * 0 ~+oo : bulkhead lcp
   * @param player1
   * @param player2
   * @return
   */
  def swapSegment(player1: Player, player2: Player): (Player, Player, Int) = {

    if (player1.contentSegment == player2.contentSegment) {
      (player1, player2, -1)
    }

    else if (player1.contentSegment > 0 && player2.contentSegment > 0) {
      if (player1.contentSegment < player2.contentSegment)
        (player1, player2, -2)
      else
        (player2, player1, -2)
    }
    else if (player1.contentSegment < 0 && player2.contentSegment < 0) {
      if (player1.contentSegment < player2.contentSegment)
        (player1, player2, -2)
      else
        (player2, player1, -2)
    }
    else {
      if (player2.contentSegment < 0)
        (player1, player2, -2)
      else
        (player2, player1, -2)
    }
  }

  /**
   * Compare suffix according to the first elements during the initialization stage of the loser tree
   */
  def arrayCompare(player1: Player, player2: Player, range: Int, unknownElement: Byte): (Int, Int, (Byte, Array[Byte], Int)) = {
    val (smaller, larger, ind) = swapArray(player1, player2)
    //content is sufficient to compare
    if (ind < larger.content.length) {
      val len = math.min(range, math.min(smaller.content.length, larger.content.length) - ind)
      val tmp2 = new Array[Byte](len)
      try {

        System.arraycopy(larger.content, ind, tmp2, 0, len)
      }
      catch {
        case _ => {
          println("copy error")
        }
      }

      (smaller.groupId, larger.groupId, (smaller.content(ind), tmp2, ind))
    }
    else {
      if (larger.existRemainItem) {
        help(smaller, larger, chaosLcpTransForm(ind), unknownElement)
      }
      else {
        if (smaller.content.length > ind)
          (smaller.groupId, larger.groupId, ((smaller.content(ind)), new Array[Byte](0), ind))

        // smaller has some but unknown element
        else if (smaller.existRemainItem)
          (smaller.groupId, larger.groupId, ((unknownElement), new Array[Byte](0), ind))

        // totally equal
        else
          (smaller.groupId, larger.groupId, null)
      }
    }
  }

  /**
   *
   * @param player1
   * @param player2
   * @return ind < contentLength means compare successful , ind >= contentLength means compare failed
   */
  def swapArray(player1: Player, player2: Player): (Player, Player, Int) = {
    var ind = 0
    while (ind < player1.content.length && ind < player2.content.length && player1.content(ind) == player2.content(ind))
      ind += 1
    if (ind < player1.content.length && ind < player2.content.length) {
      if (player1.content(ind) < player2.content(ind))
        (player1, player2, ind)
      else
        (player2, player1, ind)
    }
    else {
      if (player1.content.length > player2.content.length)
        (player1, player2, ind)
      else if (player1.content.length < player2.content.length)
        (player2, player1, ind)
      else {
        if (player1.existRemainItem)
          (player1, player2, ind)
        else
          (player2, player1, ind)
      }
    }
  }

  /**
   * Compare suffix according to lcp information
   */
  def lcpCompare(player1: Player, player2: Player, range: Int, unknownElement: Byte): (Int, Int, (Byte, Array[Byte], Int)) = {

    // totally equal to former winner
    if (player1.lcp == null)
      (player1.groupId, player2.groupId, player2.lcp)
    else if (player2.lcp == null)
      (player2.groupId, player1.groupId, player1.lcp)

    //null player
    else if (player1.lcp._2 == null)
      (player2.groupId, player1.groupId, player1.lcp)
    else if (player2.lcp._2 == null)
      (player1.groupId, player2.groupId, player2.lcp)
    //can't judge to former winner
    else if (player1.lcp._3 < 0) {
      //last solid length
      val lcp1 = chaosLcpTransForm(player1.lcp._3)

      //solid length
      val lcp2 = if (player2.lcp._3 > 0) player2.lcp._3 else chaosLcpTransForm(player2.lcp._3)
      //mustn't use >=
      if (lcp2 > lcp1)
        player2.lcp = (unknownElement, new Array[Byte](0), player1.lcp._3)
      (player1.groupId, player2.groupId, player2.lcp)
    }
    else if (player2.lcp._3 < 0) {
      val lcp2 = chaosLcpTransForm(player2.lcp._3)
      val lcp1 = if (player1.lcp._3 > 0) player1.lcp._3 else chaosLcpTransForm(player1.lcp._3)
      if (lcp1 > lcp2)
        player1.lcp = (unknownElement, new Array[Byte](0), player2.lcp._3)
      (player2.groupId, player1.groupId, player1.lcp)
    }
    else {
      val (smaller, larger, ind) = swapLcp(player1, player2)
      val lcpRangeLength = math.min(smaller.lcp._2.length, larger.lcp._2.length)

      if (ind == -1)
        (smaller.groupId, larger.groupId, larger.lcp)

      //lcp is sufficient to compare
      else if (ind >= 0 && ind < lcpRangeLength) {
        val len = math.min(range, lcpRangeLength - ind)
        try {
          val tmp2 = new Array[Byte](len)

          System.arraycopy(larger.lcp._2, ind, tmp2, 0, len)
          (smaller.groupId, larger.groupId, (smaller.lcp._2(ind), tmp2, ind + larger.lcp._3))
        }
        catch {
          case _ => {
            println("negative length")
            (smaller.groupId, larger.groupId, null)
          }
        }
      }
      //empty suffix
      else if (ind == 0 && ind == lcpRangeLength && larger.loc.prefixLen == larger.loc.suffixLength) {

        if (smaller.loc.suffixLength == smaller.loc.prefixLen)
          (smaller.groupId, larger.groupId, null)
        else if (smaller.lcp._2.length == 0)
          (smaller.groupId, larger.groupId, (unknownElement, new Array[Byte](0), 0))
        else
          (smaller.groupId, larger.groupId, (smaller.lcp._2(0), new Array[Byte](0), 0))
      }
      //empty lcp
      // lcp range contains only the last characters
      else if (ind == lcpRangeLength) {
        if (larger.existRemainItem)
          throw new Exception("larger is not end")

        if (smaller.lcp._2.length > ind)
          (smaller.groupId, larger.groupId, (smaller.lcp._2(ind), new Array[Byte](0), ind + larger.lcp._3))
        // smaller has some but unknown element
        else if (smaller.existRemainItem)
          (smaller.groupId, larger.groupId, (unknownElement, new Array[Byte](0), ind + larger.lcp._3))
        // totally equal
        else
          (smaller.groupId, larger.groupId, null)
      }
      // lcp can't judge => ind = -oo ~ -2
      else {
        if (ind >= -1)
          throw new Exception("ind > -2")
        help(smaller, larger, ind - smaller.lcp._3, unknownElement)
      }
    }
  }

  def chaosLcpTransForm(lcp: Int) = -lcp - 2

  def help(player1: Player, player2: Player, range: Int, unknownElement: Byte): (Int, Int, (Byte, Array[Byte], Int)) = {
    (player1.groupId, player2.groupId, (unknownElement, new Array[Byte](0), range))
  }

  /*
  def segmentCompare(player1: Player, player2: Player, range: Int, unknownElement: Byte, bitsPerItem: Int): (Int, Int, (Byte, Array[Byte], Int)) = {
    if(player2.contentSegment == -1) {
      (player1.groupId, player2.groupId, player1.lcp)
    }
    else if(player1.contentSegment == -1) {
      (player2.groupId, player1.groupId, player1.lcp)
    }
    else {
      val (smaller, larger, ind) = swapSegment(player1, player2)
      if(smaller.contentSegment == larger.contentSegment) {
        if (smaller.lcp == null || larger.lcp == null)
          lcpCompare(player1, player2, range, unknownElement)
        else {

          val lcpRangeLength = math.max(smaller.lcp._3 + smaller.lcp._2.length,
            larger.lcp._3 + larger.lcp._2.length)
          val suffixLength = math.min(smaller.loc.suffixLength, larger.loc.suffixLength) - smaller.loc.prefixLen

          //lcp._3 + lcp._2.length < segment
          if (ind == -1 && lcpRangeLength < 32 / bitsPerItem && suffixLength > 32 / bitsPerItem)
            help(player1, player2, chaosLcpTransForm(32 / bitsPerItem), unknownElement)
          else
            lcpCompare(player1, player2, range, unknownElement)
        }
      }
      else {
        val lcp = ItemConverter.compare(smaller.contentSegment, larger.contentSegment, bitsPerItem)
        if(lcp._1 > 0 && lcp._2 > 0 && lcp._1 > lcp._2)
          println
        if(lcp._1 < 0)
          println

        val x = if(lcp._2 == -1) new Array[Byte](0) else Array(lcp._2)
        (smaller.groupId, larger.groupId, (lcp._1, x, lcp._3))
      }
    }
  }*/

  /**
   * NOTE that only both lcp1._3 and lcp2._3 > 0 can this method be called
   * -1 lcp1._3 != lcp2._3
   * 0 ~+oo : lcp is sufficient to compare
   * -oo ~ -2 lcp length is not enough
   * @param player1
   * @param player2
   * @return
   */
  def swapLcp(player1: Player, player2: Player): (Player, Player, Int) = {
    val (lcp1, lcp2) = (player1.lcp, player2.lcp)

    assert(lcp1._3 >= 0)
    assert(lcp2._3 >= 0)

    val LcpLength = math.min(lcp1._2.length, lcp2._2.length)

    // lcp._3 different
    if (lcp1._3 < lcp2._3)
      (player2, player1, -1)
    else if (lcp1._3 > lcp2._3)
      (player1, player2, -1)
    else {
      // compare lcp._2
      var ind = 0
      while (ind < LcpLength && ind < LcpLength && player1.lcp._2(ind) == player2.lcp._2(ind))
        ind += 1

      // exist different util.item in lcp-range
      if (ind < LcpLength) {
        if (player1.lcp._2(ind) < player2.lcp._2(ind))
          (player1, player2, ind)
        else
          (player2, player1, ind)
      }
      // remain nothing
      else if (ind == 0 && LcpLength == 0 && (!player1.existRemainItem || !player2.existRemainItem)) {
        if (!player1.existRemainItem)
          (player2, player1, ind)
        else
          (player1, player2, ind)
      }
      //can't judge.
      //Send insufficient lcp to upper level, in order to enter chaos state.
      //order between players really MATTERS here.
      else {
        if (player1.lcp._2.length > player2.lcp._2.length)
          (player2, player1, chaosLcpTransForm(ind))
        else
          (player1, player2, chaosLcpTransForm(ind))
      }
    }
  }
}
