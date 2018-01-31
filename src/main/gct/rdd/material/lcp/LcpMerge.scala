package gct.rdd.material.lcp

import gct.rdd.GctConf
import gct.rdd.material.lcp.base.{Player, PlayerMaker}
import gct.rdd.smalltree.SALcpForBuild
import org.eclipse.collections.impl.list.mutable.FastList

import scala.collection.JavaConverters._

/**
 * Multi-way LCP-Merge sorting based on loser tree
 */
object LcpMerge {

  def apply(inputLcp: Array[Iterator[Player]], conf: GctConf): Array[SALcpForBuild] = {

    val playersWithGroupId = inputLcp.zipWithIndex

    var remainGroups = inputLcp.filter(_.hasNext).length

    val firstPlayers = playersWithGroupId.map { case (members, groupId) =>
      val x = members.next()
      x.groupId = groupId
      x
    }
    val res = FastList.newList[SALcpForBuild]()
    val loser = new LoserTree(inputLcp.length, conf)
    //Initialize the loser tree by all first players
    val first = loser.initTree(firstPlayers)
    res.add(new SALcpForBuild((first._3), (if (first._2.size > 0) first._2(0) else 0, 0.toByte, 0)))

    var ind = 1
    var nextGroupId = first._1
    do {
      val nextPlayer =
        if (inputLcp(nextGroupId).hasNext) {
          inputLcp(nextGroupId).next()
        }
        else {
          remainGroups -= 1
          PlayerMaker.makeNullPlayer(nextGroupId)
        }

      nextPlayer.groupId = nextGroupId
      val smallest = loser.rebuildTree(nextPlayer)
      //Filter the End Player, the location gct.rdd.top.trie.info of which is Null.
      if (smallest._3 != null)
        res.add(new SALcpForBuild(smallest._3, smallest._2))
      nextGroupId = smallest._1
      ind += 1
    } while (remainGroups > 0)
    res.asScala.toArray
  }

}
