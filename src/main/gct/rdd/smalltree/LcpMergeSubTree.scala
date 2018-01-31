package gct.rdd.smalltree

import gct.rdd.GctConf
import gct.rdd.material.lcp.base.Location
import gct.rdd.smalltree.base.{LcpOrderChecker, SmallTreeEdge, SmallTreeNode}
import util.file.FSWriterFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Suffix Tree Definition
 */
class LcpMergeSubTree(pat: Array[Byte]) extends Serializable {

  val pattern = pat

  val root = new SmallTreeNode(null)

  /**
   * Build the final suffix tree
   */
  def build(input: Array[SALcpForBuild]): SmallTreeNode = {

    val checkOrdered = LcpOrderChecker.checkFinalOrder(input, pattern)
    if (checkOrdered._1 == false) {

      val x = ("false root " + "\n")
      throw new Exception(x.toString())
    }

    //================= build suffix tree ===================
    var start = System.nanoTime()

    //todo ALERT: lcpMerge size + 1
    if (input.size == 0) throw new Exception("input size is zero!")
    val suffixLen = if (input.last.location != null) input.size else input.size - 1

    println("build suffix tree for root:" + new String(pattern) + ", num: " + suffixLen)

    val rootSeq = input(0).lcp
    val rootEdge = new SmallTreeEdge(root, getLengthWithoutPrefix(input(0).location))
    root.edges.put(rootSeq._1, rootEdge)
    rootEdge.end.SuffixInfo = input(0).location
    val stack = new mutable.Stack[SmallTreeEdge]
    stack.push(rootEdge)
    var depth = rootEdge.len

    var i = 1
    while (i < suffixLen && input(i).lcp == null) {
      rootEdge.end.addTerminalIndex(input(i).location)
      i += 1
    }

    while (i < suffixLen) {
      //继续遍历，这次是构建后缀树
      val (smallSplit, largeSplit, offset) = input(i).lcp
      var currentEdge: SmallTreeEdge = null
      var currentNode: SmallTreeNode = null

      if (stack.isEmpty) {
        val resStr = new StringBuilder

        resStr.append("false root ")

        pattern.map(x => resStr.append(x.toChar).append(" "))
        resStr.append("\n")
        resStr.append("num " + input.length + "\n")
        for (j <- i until suffixLen)
          resStr.append("index " + i + input(i).lcp)

        throw new Exception(resStr.toString())
      }

      // find current Node
      do {
        currentEdge = stack.pop()
        depth -= currentEdge.len
      } while (depth > offset)


      if (depth == offset) {
        currentNode = currentEdge.start
      }
      else {
        // split edge to build current node
        currentNode = new SmallTreeNode(currentEdge)
        val splitLen1 = offset - depth
        val splitLen2 = currentEdge.len - splitLen1

        val splitEdge = new SmallTreeEdge(currentNode, splitLen2, currentEdge.end)
        currentEdge.end.parent = splitEdge
        currentNode.edges.put(smallSplit, splitEdge)
        currentEdge.end = currentNode
        currentEdge.len = splitLen1

        stack.push(currentEdge)
        depth += currentEdge.len
      }

      // add a new edge to current node
      // terminal symbol, add a void edge
      if (getLengthWithoutPrefix(input(i).location) <= offset || input(i).lcp._2 == -1) {
        currentNode.addTerminalIndex(input(i).location)
        // deal with totally equal edge
        i += 1
        while (i < suffixLen && input(i).lcp == null) {
          currentNode.addTerminalIndex(input(i).location)
          i += 1
        }
        while (i < suffixLen && currentNode == root && input(i).location.suffixLength == input(i).location.prefixLen) {
          currentNode.addTerminalIndex(input(i).location)
          i += 1
        }
      }
      // non-terminal symbol
      else {
        val newPathLen = getLengthWithoutPrefix(input(i).location) - offset
        val newEdge = new SmallTreeEdge(currentNode, newPathLen)
        newEdge.end.SuffixInfo = input(i) location

        currentNode.edges.put(largeSplit, newEdge)
        stack.push(newEdge)
        depth += newEdge.len

        // deal with totally equal edge
        i += 1
        while (i < suffixLen && input(i).lcp == null) {
          newEdge.end.addTerminalIndex(input(i).location)
          i += 1
        }
      }
    }
    var end = System.nanoTime()
    System.err.println("Build suffix tree complete, time:" + ((end - start) / 1000000) + "ms")
    root

  }

  def getLengthWithoutPrefix(loc: Location) = loc.suffixLength - loc.prefixLen

  /**
   * Print the information of all leaf nodes (including the node depth, the string id where the suffix locates,
   * and the suffix starting position in the the corresponding string)
   */
  def printAllSuffix(prefixDepth: Int, conf: GctConf): Unit = {

    val outName = new StringBuilder
    for (i <- 0 until pattern.size)
      outName ++= (pattern(i).toInt.toString + "_")

    val res = new StringBuilder

    val out = FSWriterFactory(conf.outputLocation)
    println()
    out.open(conf.getOutputFile("subtreeForRoot_" + outName.toString()))
    var nodeNum = 0

    val q = new mutable.Queue[(SmallTreeNode, Int)]
    q += ((root, 0))
    while (q.nonEmpty) {
      val (curNode, curDepth) = q.dequeue()

      if (res.size > conf.fsWriteBufferSize) {
        out.write(res.toString())
        res.clear()
      }

      if (curNode.edges.size > 0) {
        val nextDepth = if (curNode.edges.size > 1 ||
          (curNode.edges.size == 1 && curNode.terminalSuffixInfo.size > 0))
          curDepth + 1
        else curDepth
        curNode.edges.keySet.map { k =>
          val childNode = curNode.edges.get(k).get.end
          q += ((childNode, nextDepth))
        }
      }

      if (curNode.edges.size <= 0) {

        if (curNode.terminalSuffixInfo.size <= 0) {
          res ++= ((prefixDepth + curDepth) + " " + curNode.SuffixInfo.fileName + ":" + curNode.SuffixInfo.startInd + "\n")
          nodeNum += 1
        } else {
          res ++= ((prefixDepth + curDepth + 1) + " " + curNode.SuffixInfo.fileName + ":" + curNode.SuffixInfo.startInd + "\n")
          nodeNum += 1
          curNode.terminalSuffixInfo.get.asScala.foreach { loc =>
            res ++= ((prefixDepth + curDepth + 1) + " " + loc.fileName + ":" + loc.startInd + "\n")
            nodeNum += 1
          }
        }
      } else {
        if (curNode.terminalSuffixInfo.size > 0) {
          curNode.terminalSuffixInfo.get.asScala.foreach { loc =>
            res ++= ((prefixDepth + curDepth + 1) + " " + loc.fileName + ":" + loc.startInd + "\n")
            nodeNum += 1
          }
        }
      }
    }

    out.write(res.toString())
    res.clear()

    out.close()
  }
}
