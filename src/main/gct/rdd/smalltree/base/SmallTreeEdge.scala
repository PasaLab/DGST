package gct.rdd.smalltree.base

/**
 * Tree Edge Definition
 */
class SmallTreeEdge(val start: SmallTreeNode, var len: Long, var end: SmallTreeNode = null, var label: Option[Array[Byte]] = None) extends Serializable {
  if (end == null)
    end = new SmallTreeNode(this)
}
