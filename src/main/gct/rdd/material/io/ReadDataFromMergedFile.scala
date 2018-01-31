package gct.rdd.material.io

import org.eclipse.collections.impl.list.mutable.FastList
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import util.file.FSReaderFactory

import scala.collection.mutable

/**
 * Read original data sequentially from the merged file in the sub-tree construction stage
 */
object ReadDataFromMergedFile {

  def apply(targetsOrgnizedByIO: mutable.HashMap[String, FastList[Long]],
            buffer: Array[Byte],
            bufferSizeForEachSuffix: Int,
            fileName2StartIndMapper: UnifiedMap[String, (Long, Long)],
            fsBufferSize: Int, sortingBufferSize: Int,
            mergedFilePath: String): UnifiedMap[(String, Long), Int] = {

    if (fileName2StartIndMapper == null)
      println("ReadDataFromMergedFile:: fileName2StartIndMapper  = null")

    val mergeFileIndList = FastList.newList[(Long, Long)]
    val Input2MergedFileIndMapper = UnifiedMap.newMap[Long, (String, Long)]

    targetsOrgnizedByIO.foreach { case (fileName, startlocs) =>
      val (start, end) = fileName2StartIndMapper.get(fileName)
      var i = 0
      while (i < startlocs.size()) {

        val key = start + startlocs.get(i)
        Input2MergedFileIndMapper.put(key, (fileName, startlocs.get(i)))
        mergeFileIndList.add((key, end))
        i += 1
      }
    }

    val tmpRes = apply(mergeFileIndList, buffer, bufferSizeForEachSuffix,
      fsBufferSize, sortingBufferSize, mergedFilePath)

    val res = UnifiedMap.newMap[(String, Long), Int](mergeFileIndList.size())
    val iter = tmpRes.entrySet().iterator()
    while (iter.hasNext) {
      val xx = iter.next()
      res.put(Input2MergedFileIndMapper.get(xx.getKey), xx.getValue)
    }
    res
  }

  def apply(targetsOrgnizedByIO: FastList[(Long, Long)],
            buffer: Array[Byte],
            bufferSizeForEachSuffix: Int,
            fsBufferSize: Int, sortingBufferSize: Int,
            mergedFilePath: String
             ): UnifiedMap[Long, Int] = {

    val fsReadBuffer = new Array[Byte](fsBufferSize)
    var bufferStart = 0
    val locs2BufferStartMapper = UnifiedMap.newMap[Long, Int]
    //Sort according to the global location in the merged string
    targetsOrgnizedByIO.sortThis(new SuffixRangeComparator)
    val reader = FSReaderFactory(mergedFilePath)
    var currentFSReadEnd: Long = 0
    var startLocInd = 0
    var bytesReadThisTime: Long = 0

    val gapRecorder = FastList.newList[(Int, Int)]
    bytesReadThisTime = reader.read(fsReadBuffer)

    while ((bytesReadThisTime > -1 && startLocInd < targetsOrgnizedByIO.size()) || gapRecorder.size() > 0) {
      val fsBufferStartInd = currentFSReadEnd
      currentFSReadEnd += bytesReadThisTime
      // first fill undone tasks between buffers
      var remainingReadTaskNum = gapRecorder.size()
      while (remainingReadTaskNum > 0) {
        remainingReadTaskNum -= 1
        val (tmpStart, remainingLen) = gapRecorder.get(remainingReadTaskNum)
        val tmpLen = math.min(fsReadBuffer.size, remainingLen)
        System.arraycopy(fsReadBuffer, 0, buffer, tmpStart, tmpLen)
        if (remainingLen > tmpLen)
          gapRecorder.set(remainingReadTaskNum, (tmpStart + tmpLen, remainingLen - tmpLen))
        else
          gapRecorder.remove(remainingReadTaskNum)
      }

      // locs within this buffer
      while (startLocInd < targetsOrgnizedByIO.size() && targetsOrgnizedByIO.get(startLocInd)._1 <= currentFSReadEnd) {
        val (targetStart, fileEndInd) = targetsOrgnizedByIO.get(startLocInd)
        //get fs data
        val fsBufferStartForCopy = (targetStart - fsBufferStartInd).toInt
        val sizeForThisSuffix = math.min(bufferSizeForEachSuffix, fileEndInd - targetStart)
        val fsBufferLengthForCopy = math.min(currentFSReadEnd - targetStart, sizeForThisSuffix)
        var i = 0
        while (i < fsBufferLengthForCopy) {
          buffer(bufferStart + i) = fsReadBuffer(fsBufferStartForCopy + i)
          i += 1
        }
        if (fsBufferLengthForCopy < sizeForThisSuffix)
          gapRecorder.add((bufferStart + i, (sizeForThisSuffix - fsBufferLengthForCopy).toInt))

        while (i < bufferSizeForEachSuffix) {
          buffer(bufferStart + i) = -1
          i += 1
        }
        locs2BufferStartMapper.put(targetsOrgnizedByIO.get(startLocInd)._1, bufferStart)
        bufferStart += bufferSizeForEachSuffix
        startLocInd += 1
      }

      // read next block
      bytesReadThisTime = reader.read(fsReadBuffer)

    }

    locs2BufferStartMapper
  }

}
