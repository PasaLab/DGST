package gct.rdd.material.io

import org.eclipse.collections.impl.list.mutable.FastList
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import util.file.FSReaderFactory

import scala.collection.mutable

/**
 * Read original sequential data from multiple files in the sub-tree construction stage
 */
object ReadDataFromFile {

  def apply(targetsOrgnizedByIO: mutable.HashMap[String, FastList[Int]],
            buffer: Array[Byte],
            bufferSizeForEachSuffix: Int,
            fsBufferSize: Int, sortingBufferSize: Int,
            unknownElement: Byte,
            inputDir: String): UnifiedMap[(String, Int), Int] = {

    val fsReadBuffer = new Array[Byte](fsBufferSize)
    var bufferStart = 0

    val locs2BufferStartMapper = UnifiedMap.newMap[(String, Int), Int]

    targetsOrgnizedByIO.foreach { case (fileName, startlocs) =>
      startlocs.sortThis()

      val reader = FSReaderFactory(inputDir + fileName)
      var currentFSBufferEnd = 0

      var startLocInd = 0

      while (currentFSBufferEnd >= 0 && startLocInd < startlocs.size()) {
        val fsBufferStartInd = currentFSBufferEnd
        currentFSBufferEnd += reader.read(fsReadBuffer)

        // todo gap between buffers

        // locs within this buffer
        while (startLocInd < startlocs.size() && startlocs.get(startLocInd) < currentFSBufferEnd) {
          //Get file system data
          val fsBufferStartForCopy = startlocs.get(startLocInd) - fsBufferStartInd
          val fsBufferLengthForCopy = math.min(currentFSBufferEnd - startlocs.get(startLocInd), bufferSizeForEachSuffix)

          var i = 0
          while (i < fsBufferLengthForCopy) {
            buffer(bufferStart + i) = fsReadBuffer(fsBufferStartForCopy + i)
            i += 1
          }

          while (i < bufferSizeForEachSuffix) {
            buffer(bufferStart + i) = -1
            i += 1
          }
          locs2BufferStartMapper.put((fileName, startlocs.get(startLocInd)), bufferStart)
          bufferStart += bufferSizeForEachSuffix
          startLocInd += 1
        }

      }

    }

    locs2BufferStartMapper
  }

}
