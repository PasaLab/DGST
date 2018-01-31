package gct.rdd.top.trie

import gct.rdd.top.trie.info.Info
import org.eclipse.collections.impl.list.mutable.FastList
import org.eclipse.collections.impl.map.mutable.UnifiedMap

/**
 * Trie Node Definition
 */
class TrieNode(pa: TrieNode, nodeInfo: Info) extends Serializable {
  val parent: TrieNode = pa
  val edges: UnifiedMap[Byte, TrieNode] = UnifiedMap.newMap()
  var info = nodeInfo
  var count: Long = 0
  var shouldStop = false

  def this() {
    this(null, null)
  }

  def this(info: Info) {
    this(null, info)
  }

  def this(pa: TrieNode) {
    this(pa: TrieNode, null)
  }

  def this(pa: TrieNode, tagging: Boolean) {
    this(pa)
    shouldStop = tagging
  }

  def addIndex(pair: (String, Int)): Unit = {
    if (info == null)
      info = new Info
    info.suffixIndices.addIndex(pair)
  }

  def addTerminalIndex(pair: (String, Long)): Unit = {
    if (info == null)
      info = new Info
    info.terminalSuffixIndices.addIndex(pair)
  }

  def addTerminalIndex(list: FastList[(String, Long)]): Unit = {
    if (info == null)
      info = new Info
    info.terminalSuffixIndices.addIndex(list)
  }
}
