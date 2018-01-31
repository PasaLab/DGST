package gct.rdd.material.lcp

import gct.rdd.GctConf
import gct.rdd.material.lcp.base.{Location, Player}

/**
 * Loser tree for LCP-Merge sorting
 */
class LoserTree(groups: Int, conf: GctConf) {

  val groupNum = groups
  val tree = Array.fill(groupNum)(-1)
  val LcpRange = conf.range
  val unknownElement = conf.unknownElement
  val bitsPerItem = conf.bitsPerItem
  val currentPlayers = new Array[Player](groupNum)
  // encoded solid lcp
  var solidLcpLength: Int = 0
  var remainGroups = groupNum

  def initTree(players: Array[Player]): (Int, Array[Byte], Location) = {
    var ind = groupNum
    players.foreach { case player =>
      currentPlayers(player.groupId) = player
    }

    val winner = groupNum match {
      case 1 => {
        tree(0) = 0
        currentPlayers(0)
      }
      case _ => {
        while (ind > 0) {
          ind -= 1
          addPlayer(ind)
        }
        currentPlayers(tree(0))
      }
    }
    (winner.groupId, winner.content, winner.loc)
  }

  /**
   * Add a new player
   */
  def addPlayer(playerInd: Int): Unit = {
    var player = currentPlayers(playerInd)
    var parent = (playerInd + groupNum) / 2
    var winner = playerInd
    while (parent > 0) {
      if (tree(parent) < 0) {
        tree(parent) = winner
        parent = 0
      }
      else {
        val opp = currentPlayers(tree(parent))
        val res = SeqComparator.arrayCompare(player, opp, LcpRange, unknownElement)
        winner = res._1
        player = currentPlayers(winner)
        currentPlayers(res._2).lcp = res._3
        tree(parent) = res._2
      }
      parent /= 2
    }
    tree(0) = winner
  }

  def rebuildTree(input: Player): (Int, (Byte, Byte, Int), Location) = {
    var player = input
    val playerInd = player.groupId

    //Check the chaos state of the loser tree
    checkLcpRange(player)
    chaosTransformation(player)

    currentPlayers(playerInd) = player

    tree(0) = tree.size match {
      case 1 => {
        0
      }
      case _ => {
        var parent = (playerInd + groupNum) / 2
        var winner = playerInd
        while (parent > 0) {
          if (tree(parent) < 0) {
            tree(parent) = playerInd
            winner = playerInd
          }
          else {
            val opp = currentPlayers(tree(parent))
            checkLcpRange(opp)

            chaosTransformation(opp)

            /*val res = SeqComparator.segmentCompare(player, opp, LcpRange, unknownElement, bitsPerItem)*/
            val res = SeqComparator.lcpCompare(player, opp, LcpRange, unknownElement)
            winner = res._1
            player = currentPlayers(winner)
            currentPlayers(res._2).lcp = res._3
            tree(parent) = res._2
          }
          parent /= 2
        }
        winner
      }
    }

    val winnerPlayer = currentPlayers(tree(0))

    if (winnerPlayer.lcp != null) {
      //leave chaos state
      if (winnerPlayer.lcp._3 >= 0) {
        solidLcpLength = winnerPlayer.lcp._3
      }
      // enter chaos state and solidLcpLength is negative.
      if (winnerPlayer.lcp._3 < -1) {
        solidLcpLength = math.min(winnerPlayer.lcp._3, -conf.segmentLen)
      }
    }

    val resLcp = winnerPlayer.lcp match {
      case null => null
      case _ => {
        val lcp2 = winnerPlayer.lcp._2 match {
          case null => 255.toByte
          case x => if (x.size == 0) 255.toByte else winnerPlayer.lcp._2(0)
        }
        (winnerPlayer.lcp._1, lcp2, winnerPlayer.lcp._3)
      }
    }
    (winnerPlayer.groupId, resLcp, winnerPlayer.loc)
  }

  def chaosTransformation(player: Player): Unit = {
    if (player.lcp != null && solidLcpLength < 0) {
      val x = -solidLcpLength - 2
      if (player.lcp._3 >= x || (player.lcp._3 < 0 && player.lcp._3 < solidLcpLength))
        player.lcp = (unknownElement, Array[Byte](0), solidLcpLength)
    }
  }

  def checkLcpRange(player: Player): Unit = {
    if (player.lcp != null && player.lcp._2 != null && player.lcp._2.size > 0 && player.lcp._2(0) == -1)
      throw new Exception("lcp range starting with -1")
  }
}
