package gct.rdd.smalltree.base

import gct.rdd.material.lcp.base.Location
import org.eclipse.collections.impl.list.mutable.FastList

import scala.collection._

/**
 * Tree Node Definition
 */
class SmallTreeNode(var parent: SmallTreeEdge, val edges: mutable.Map[Byte, SmallTreeEdge] = new mutable.HashMap[Byte, SmallTreeEdge]) extends Serializable {
  var terminalSuffixInfo: Option[FastList[Location]] = None
  var SuffixInfo: Location = _

  def addTerminalIndex(input: Location): Unit = {
    if (terminalSuffixInfo.isEmpty)
      terminalSuffixInfo = Some(FastList.newList[Location])
    terminalSuffixInfo.get.add(input)
  }
}
