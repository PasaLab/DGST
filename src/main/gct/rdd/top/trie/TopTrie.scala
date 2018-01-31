package gct.rdd.top.trie

import gct.rdd.divide.CollectInfo
import org.eclipse.collections.impl.list.mutable.FastList
import util.file.FSWriterFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Frequency Trie for S-prefix frequency counting
 */
class TopTrie extends Serializable {

  var fileName: String = ""

  var root: TrieNode = new TrieNode()
  root.shouldStop = true

  /**
   * Count the S-prefix based on input file.
   * This method is used in ERa algorithm
   */
  def addOnlyLeaf(fileName: String, content: Array[Byte], startInd: Int, endInd: Int, offset: Long, terminalFlag: Boolean): TrieNode = {
    var index = startInd
    var currentNode = root
    while (index < endInd) {
      val entry = currentNode.edges.get(content(index))
      if (entry == null) {
        val node = new TrieNode(currentNode)
        currentNode.edges.put(content(index), node)
        currentNode = node
      }
      else {
        currentNode = entry
        if (currentNode.shouldStop == true)
          return null
      }
      index = index + 1
    }
    if (terminalFlag)
      currentNode.addTerminalIndex((fileName, startInd + offset))
    currentNode.count += 1
    currentNode
  }

  /**
   * Count the S-prefix based on input split.
   * This method is used in DGST algorithm.
   */
  def addWithSplit(fileName: String, content: Array[Byte], startInd: Int, endInd: Int,
                   currentLen: Int, targetLen: Int, offset: Long = 0, fileEndFlag: Boolean = false): TrieNode = {
    var currentNode = root
    var ind = startInd
    while (ind < endInd) {
      val next = currentNode.edges.get(content(ind))

      // prefix not in targetList
      if (ind - startInd < targetLen && next == null) {
        return null
      }
      else {
        if (next == null) {
          val newNode = new TrieNode(currentNode)
          currentNode.edges.put(content(ind), newNode)
          currentNode = newNode
        }
        else
          currentNode = next

        if (currentNode.shouldStop == false) {
          currentNode.count += 1
        }
        ind += 1
      }

    }
    if (fileEndFlag)
      currentNode.addTerminalIndex((fileName, startInd + offset))

    currentNode
  }

  def buildWithFinalResult(roots: FastList[(String, Long)],
                           terminalInfo: Array[(String, FastList[(String, Long)])],
                           maxCount: Int): Unit = {

    if (root.edges.size() > 0)
      root.edges.clear()

    var i = 0
    while (i < roots.size()) {
      add(roots.get(i)._1.getBytes, roots.get(i)._2)
      i += 1
    }
    i = 0
    while (i < terminalInfo.size) {
      var leaf = find(terminalInfo(i)._1.getBytes)
      // new Character in the end
      var newLeafFlag = false
      if (leaf == null) {
        newLeafFlag = true
        leaf = add(terminalInfo(i)._1.getBytes, terminalInfo(i)._2.size())
      }

      val terminalNum = terminalInfo(i)._2.size()
      leaf.addTerminalIndex(terminalInfo(i)._2)

      // ensure accuracy
      while (!newLeafFlag && leaf.parent != null) {
        leaf.count += terminalNum
        leaf = leaf.parent
      }
      i += 1
    }
    prune(maxCount)
  }

  //============================= build Main Trie =============
  def add(content: Array[Byte], count: Long): TrieNode = {
    var index = 0
    var currentNode = root
    val endInd = content.length
    while (index < endInd) {
      val entry = currentNode.edges.get(content(index))
      if (entry == null) {
        val node = new TrieNode(currentNode)
        currentNode.edges.put(content(index), node)
        currentNode = node
      }
      else {
        currentNode = entry
      }
      currentNode.count += count

      index = index + 1
    }
    currentNode
  }

  /**
   * Prune the tree with budget, and return new targets
   */
  def prune(budget: Int): FastList[(Array[Byte])] = {

    val q = new mutable.Queue[(Array[Byte], TrieNode)]()
    q += ((Array(), root))
    val res = FastList.newList[Array[Byte]]()

    while (q.nonEmpty) {
      val (seq, node) = q.head
      q.dequeue()
      if (node.info != null)
        node.info.toArray

      if (node.count > budget || node.parent == null) {
        if (node.edges.size() <= 0)
          res.add(seq)
        else {
          node.edges.entrySet().asScala.foreach { entry =>
            val buf = new ArrayBuffer[Byte]
            buf ++= seq
            val (k, v) = (entry.getKey, entry.getValue)
            q += ((buf.+=(k).toArray, v))
          }
        }
      }
      else {
        node.edges.clear()
      }
    }
    res

  }

  /**
   * Find the node correspond to input sequence
   */
  def find(input: Array[Byte]): TrieNode = {

    var currentNode = root
    var index = 0
    while (index < input.size) {
      val entry = currentNode.edges.get(input(index))
      if (entry == null) {
        return null
      }
      else {
        currentNode = entry
      }
      index = index + 1
    }
    currentNode
  }

  // ===================== Merge & prone ================================
  //todo: merge existing tree, avoid overlapping
  def merge(that: TopTrie): TopTrie = {
    that.root.edges.entrySet().asScala.foreach { entry =>
      val (key, value) = (entry.getKey, entry.getValue)
      merge(root, value, key)
    }
    this
  }

  def merge(ori: TrieNode, that: TrieNode, key: Byte): Unit = {
    val next = ori.edges.get(key)
    if (next == null) {
      ori.edges.put(key, that)
      return
    }
    next.count += that.count

    //todo check correctness
    if (next.info != null && that.info != null)
      next.info.merge(that.info)

    that.edges.entrySet().asScala.foreach { entry =>
      val (k, v) = (entry.getKey, entry.getValue)
      merge(next, v, k)
    }
  }

  def shouldProceed(maxCount: Int): Boolean = {
    var flag = false

    val q = new mutable.Queue[TrieNode]()
    q += root

    while (q.nonEmpty) {
      val node = q.head
      q.dequeue()

      if (node.edges.size() > 0) {
        node.edges.values().asScala.foreach(q += _)
      }
      else {
        if (node.count <= maxCount)
          node.shouldStop = true
        else
          flag = true
      }
    }
    flag
  }

  /**
   * Rebuild tries of every worker using targets.
   *
   * @param targets
   */
  def tag(targets: FastList[String]): Unit = {
    if (targets.size() <= 0)
      return

    val newRoot = new TrieNode()
    newRoot.shouldStop = true

    targets.asScala.foreach { inst =>
      val input = inst.getBytes()

      var index = 0
      var currentNode = newRoot
      while (index < input.size) {
        val entry = currentNode.edges.get(input(index))
        if (entry == null) {
          val node = new TrieNode(currentNode, true)
          currentNode.edges.put(input(index), node)
          currentNode = node
        }
        else {
          currentNode = entry
        }
        index = index + 1
      }
    }
    root.edges.clear()
    root = newRoot

  }

  //========================= get output =====================
  def getAllRoots(): FastList[(String, Long)] = {

    val q = new mutable.Queue[(Array[Byte], TrieNode)]()
    q += ((Array(), root))
    val res = FastList.newList[(String, Long)]()

    while (q.nonEmpty) {
      val (seq, node) = q.dequeue()

      if (node.edges.size() <= 0) {
        if (node.count <= 0) {
          println(new String(seq.map(x => (x + '0').toByte)))
        }
        else
          res.add((new String(seq), node.count))
      }
      else {
        node.edges.entrySet().asScala.foreach { entry =>
          val buf = new ArrayBuffer[Byte]
          buf ++= (seq)
          val (k, v) = (entry.getKey, entry.getValue)
          q += ((buf.+=(k).toArray, v))
        }
      }
    }
    res
  }

  def getCollectInfo(): FastList[(String, CollectInfo)] = {

    val q = new mutable.Queue[(Array[Byte], TrieNode)]()
    q += ((Array(), root))
    val res = FastList.newList[(String, CollectInfo)]()

    while (q.nonEmpty) {
      val (seq, node) = q.dequeue()
      // add children
      if (node.edges.size() > 0) {
        node.edges.entrySet().asScala.foreach { entry =>
          val buf = new ArrayBuffer[Byte]
          buf ++= (seq)
          val (k, v) = (entry.getKey, entry.getValue)
          q += ((buf.+=(k).toArray, v))
        }
      }
      // collect gct.rdd.top.trie.info
      if (node.edges.size() <= 0 || (node.info != null && node.info.terminalSuffixIndices.size() > 0)) {
        val tmp = new CollectInfo
        if (node.info != null && node.info.terminalSuffixIndices.size() > 0) {
          tmp.terminalLoc = node.info.terminalSuffixIndices.buffer
        }
        if (node.edges.size() <= 0) {
          //todo find out what happened
          if (node.count <= 0) {
            if (node.shouldStop == false)
              println(new String(seq.map(x => (x + '0').toByte)))
          }
          else
            tmp.count = node.count
        }
        res.add((new String(seq), tmp))
      }
    }
    res
  }

  def getAllTerminals(): FastList[(String, FastList[(String, Long)])] = {

    val q = new mutable.Queue[(Array[Byte], TrieNode)]()
    q += ((Array(), root))
    val terminals = FastList.newList[(String, FastList[(String, Long)])]()

    while (q.nonEmpty) {
      val (seq, node) = q.dequeue()

      if (node.info.terminalSuffixIndices.size() > 0)
        terminals.add((new String(seq), node.info.terminalSuffixIndices.buffer))

      if (node.edges.size() > 0) {

        node.edges.entrySet().asScala.foreach { entry =>
          val buf = new ArrayBuffer[Byte]
          buf ++= (seq)
          val (k, v) = (entry.getKey, entry.getValue)
          q += ((buf.+=(k).toArray, v))
        }
      }
    }
    terminals
  }

  def print(outLocation: String): Array[(String, Int)] = {

    val writer = FSWriterFactory(outLocation)
    writer.open(outLocation)

    val q = new mutable.Queue[(Array[Byte], TrieNode, Int)]()
    q += ((Array(), root, 0))
    val res = new ArrayBuffer[(String, Int)]()

    while (q.nonEmpty) {
      val (seq, node, depth) = q.dequeue()

      // add children
      if (node.edges.size() > 0) {
        val nextDepth = if (node.edges.size() > 1 ||
          (node.edges.size() == 0 && node.info.terminalSuffixIndices.size() > 0))
          depth + 1
        else depth

        node.edges.entrySet().asScala.foreach { entry =>
          val buf = new ArrayBuffer[Byte]
          buf ++= (seq)
          val (k, v) = (entry.getKey, entry.getValue)
          q += ((buf.+=(k).toArray, v, nextDepth))
        }
      }
      //needn't output terminal suffix in a leaf node
      if (node.edges.size() > 0 && node.info != null && node.info.terminalSuffixIndices.size() > 0) {
        var i = 0
        while (i < node.info.terminalSuffixIndices.size()) {
          val tmp = node.info.terminalSuffixIndices.buffer.get(i)
          writer.write((depth + 1) + " " + tmp._1 + ":" + tmp._2 + "\n")
          i += 1
        }
      }
      if (node.edges.size() <= 0)
        res += ((new String(seq), depth))
    }
    writer.close()
    res.toArray
  }
}
