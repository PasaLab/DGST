package util.matching

import gct.rdd.GctConf
import gct.rdd.divide.SegmentBasedPrefixInfo
import gct.rdd.input.InputSplit
import gct.rdd.material.lcp.base.{Location, SharedBufferSuffix}
import org.eclipse.collections.impl.list.mutable.FastList
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import util.file.FSReaderFactory

import scala.collection.JavaConverters._
import scala.collection._

/**
 * A TrieGraph Algorithm for multi-pattern matching
 */
object TrieGraph extends PatternMatchingStrategy {

  def findAllOccurrencesInSplit(prefixes: Array[Array[Byte]], split: InputSplit, conf: GctConf)
  : Array[SegmentBasedPrefixInfo] = {
    val prefixNum = prefixes.length
    val res = new Array[SegmentBasedPrefixInfo](prefixNum)
    var ind = 0
    while (ind < prefixNum) {
      res(ind) = new SegmentBasedPrefixInfo(ind, prefixes(ind).length)
      ind += 1
    }

    val root = initTrieGraph(prefixes, conf.alphabetNum)

    var segmentInd = 0

    var contentInd = 0

    var currentFileStartInd = 0
    while (segmentInd < split.segments.length) {
      val currentFileSegment = split.segments(segmentInd)
      var currentNode = root
      while (contentInd - currentFileStartInd < currentFileSegment.len) {
        val key = split.content(contentInd)

        currentNode = currentNode.link.get(key) match {
          case None => currentNode.link.get(key).get
          case Some(node) => node
        }

        var checkPath = currentNode
        while (checkPath.parent != None) {
          if (checkPath.ind >= 0) {
            val startInd = contentInd - currentFileStartInd - prefixes(checkPath.ind).length + 1 + currentFileSegment.start
            if (startInd < 0)
              throw new Exception("startInd < 0")
            val loc = new Location(currentFileSegment.fileName, startInd,
              currentFileSegment.getFileTotalLen - startInd, prefixes(checkPath.ind).length)

            res(checkPath.ind).loc.add(new SharedBufferSuffix(split.content, contentInd - prefixes(checkPath.ind).length + 1, loc))
          }
          checkPath = checkPath.relatedNode
        }
        contentInd += 1
      }
      if (segmentInd + 1 >= split.segments.length && currentFileSegment.remainNum > 0) {
        val extraMax = prefixes.map(_.length).max
        while (contentInd < split.mergeFileLen + extraMax) {
          val key = split.content(contentInd)

          currentNode = currentNode.link.get(key) match {
            case None => currentNode.link.get(key).get
            case Some(node) => node
          }

          var checkPath = currentNode
          while (checkPath.parent != None) {
            if (checkPath.ind >= 0) {
              val startInd = contentInd - currentFileStartInd - prefixes(checkPath.ind).length + 1 + currentFileSegment.start
              if (startInd - currentFileSegment.start < currentFileSegment.len) {
                val loc = new Location(currentFileSegment.fileName, startInd,
                  currentFileSegment.getFileTotalLen - startInd, prefixes(checkPath.ind).length)
                res(checkPath.ind).loc.add(new SharedBufferSuffix(split.content, contentInd - prefixes(checkPath.ind).length + 1, loc))
              }

            }
            checkPath = checkPath.relatedNode
          }
          contentInd += 1

        }
      }

      currentFileStartInd = contentInd
      segmentInd += 1

    }
    res

  }

  /**
   * Find all location of S-prefix
   */
  def findAllLocs(prefixes: Array[Array[Byte]], alphabetNum: Int,
                  fileName2StartIndMapper: UnifiedMap[String, (Long, Long)],
                  conf: GctConf): Array[(String, FastList[Location])] = {
    val prefixNum = prefixes.length
    val res = new Array[(String, FastList[Location])](prefixNum)
    var ind = 0
    while (ind < prefixNum) {
      res(ind) = (new String(prefixes(ind)), FastList.newList[Location])
      ind += 1
    }
    val root = initTrieGraph(prefixes, alphabetNum)

    val buffer = new Array[Byte](conf.fsReadBufferSize)
    val reader = FSReaderFactory(conf.workingDir + "/" + conf.MergedFileName)

    val x = fileName2StartIndMapper.entrySet().asScala.foreach { x =>
      val fileName = x.getKey
      val (fileStart: Long, fileEnd: Long) = x.getValue
      var fileInd = fileStart

      reader.seek(fileStart)
      var currentReadSize = reader.read(buffer)

      ind = 0
      var currentNode = root

      while (fileInd < fileEnd) {
        if (ind >= currentReadSize) {
          ind = 0
          currentReadSize = reader.read(buffer)
        }
        val key = buffer(ind)

        currentNode = currentNode.link.get(key) match {
          case None => currentNode.link(key)
          case Some(node) => node
        }
        var checkPath = currentNode
        while (checkPath.parent.isDefined) {
          if (checkPath.ind >= 0) {
            val startInd = fileInd - prefixes(checkPath.ind).length + 1
            if (startInd < 0)
              throw new Exception("startInd < 0")
            res(checkPath.ind)._2.add(new Location(fileName, startInd - fileStart, fileEnd - startInd, prefixes(checkPath.ind).length))
          }
          checkPath = checkPath.relatedNode
        }

        ind += 1
        fileInd += 1
      }
    }
    res
  }

  def initTrieGraph(prefixes: Array[Array[Byte]], alphabetNum: Int): TrieGraphNode = {
    val root = new TrieGraphNode(None)
    prefixes.zipWithIndex.foreach { case (input, ind) =>
      var currentNode = root

      var i = 0
      while (i < input.length) {
        currentNode.link.get(input(i)) match {
          case Some(xx: TrieGraphNode) => currentNode = xx
          case None => {
            currentNode.link.put(input(i), new TrieGraphNode(Some(currentNode), Some(input(i))))
            currentNode = currentNode.link.get(input(i)).get
          }
        }
        i += 1
      }
      currentNode.ind = ind
    }

    // bfs to build failLinks & relatedNodes
    val q = new mutable.Queue[TrieGraphNode]
    q += root
    while (q.nonEmpty) {
      val currentNode = q.dequeue()

      currentNode.link.foreach { case (key, value) =>
        currentNode.link.put(key, value)
        q += value
      }

      currentNode.parent match {
        case None => {
          currentNode.relatedNode = currentNode
          var i = 0
          while (i < alphabetNum) {
            val key = i.toByte
            if (currentNode.link.get(key).isEmpty)
              currentNode.link.put(key, currentNode)
            i += 1
          }
        }
        case Some(parent) => {
          val relatedNode = parent.parent match {
            case None => parent.relatedNode
            case Some(node) => parent.relatedNode.link(currentNode.incomingElement.get)
          }
          currentNode.relatedNode = relatedNode
          var i = 0
          while (i < alphabetNum) {
            val key = i.toByte
            if (currentNode.link.get(key).isEmpty)
              currentNode.link.put(key, relatedNode.link.get(key).get)
            i += 1
          }
        }
      }
    }
    root
  }

  class TrieGraphNode(val parent: Option[TrieGraphNode],
                      var incomingElement: Option[Byte] = None,
                      val link: mutable.HashMap[Byte, TrieGraphNode] = new mutable.HashMap[Byte, TrieGraphNode]
                       ) {
    var ind: Int = -1
    var relatedNode: TrieGraphNode = _
  }

}
