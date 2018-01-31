package gct.rdd.top.trie.info

/**
 * Some Information about node
 */
class Info extends Serializable {
  val suffixIndices: WrappedBuffer[(String, Int)] = new WrappedBuffer[(String, Int)]
  val terminalSuffixIndices: WrappedBuffer[(String, Long)] = new WrappedBuffer[(String, Long)]

  def merge(that: Info): Unit = {
    suffixIndices.merge(that.suffixIndices)
    terminalSuffixIndices.merge(that.terminalSuffixIndices)
  }

  def toArray(): Info = {
    this
  }
}
