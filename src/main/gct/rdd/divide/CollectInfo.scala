package gct.rdd.divide

import org.eclipse.collections.impl.list.mutable.FastList

case class CollectInfo(var count: Long = 0, var terminalLoc: FastList[(String, Long)] = null) {
  def +(that: CollectInfo): CollectInfo = {
    val resCount = this.count + that.count
    val resLocs = FastList.newList[(String, Long)]()
    if (this.terminalLoc != null)
      resLocs.addAll(this.terminalLoc)
    if (that.terminalLoc != null)
      resLocs.addAll(that.terminalLoc)
    if (resLocs.size() > 0)
      new CollectInfo(resCount, resLocs)
    else
      new CollectInfo(resCount, null)
  }
}
