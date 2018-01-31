package gct.rdd.input

import gct.rdd.GctConf
import org.eclipse.collections.impl.list.mutable.FastList
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import util.file.{FSReaderFactory, FSWriterFactory, InputStatus, ListFiles}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Merge all input string into a merged string
 */
object MergeDirToFile {

  def mergeAndSplit(path: String, conf: GctConf): (UnifiedMap[String, (Long, Long)], Array[InputSplit]) = {

    val names = ListFiles(path)

    val lengths = names.map(x => (conf.getFileName(x), InputStatus.getLength(x))).toMap
    val inputLenSum = lengths.map(_._2).sum
    // generate content range for every node
    val rangePerNode = ((inputLenSum + conf.partition - 1) / conf.partition).toInt

    val inputSplits = FastList.newList[InputSplit]
    var currentSplit = FastList.newList[FileSegment]
    var lenForCurrentSplit = rangePerNode
    val outputPath = conf.mergedFilePath

    val writer = FSWriterFactory(outputPath)
    writer.open(outputPath)
    val readBuffer = new Array[Byte](conf.fsReadBufferSize)

    // mark every file start index in MergedFile
    var outLocStart = 0L
    val fileName2MergeStartMapper = UnifiedMap.newMap[String, (Long, Long)](names.size)

    names.foreach { fileName =>

      //mark IN-FILE-INDEX for current Split
      var startIndForCurrentSplit: Long = 0

      val reader = FSReaderFactory(fileName)

      //mark readSum using readBuffer
      var totalReadNum: Long = 0
      var readSize = reader.read(readBuffer)
      var haveReadNumForCurrentSplit = 0
      while (readSize > 0) {
        if (readSize >= lenForCurrentSplit) {

          //remain Num for current File
          var remainNum = lengths(conf.getFileName(fileName)) - totalReadNum

          var remainReadSize = readSize
          while (remainReadSize >= lenForCurrentSplit) {
            //todo remainNum correctness
            remainNum -= lenForCurrentSplit
            currentSplit.add(new FileSegment(conf.getFileName(fileName), startIndForCurrentSplit, lenForCurrentSplit + haveReadNumForCurrentSplit, remainNum))
            remainReadSize -= lenForCurrentSplit
            val currentMergedIndex = rangePerNode.toLong * inputSplits.size()
            inputSplits.add(new InputSplit(currentMergedIndex, rangePerNode, currentSplit.asScala.toArray))
            currentSplit.clear()
            startIndForCurrentSplit += lenForCurrentSplit + haveReadNumForCurrentSplit
            lenForCurrentSplit = rangePerNode
            haveReadNumForCurrentSplit = 0
          }
          lenForCurrentSplit -= remainReadSize
          haveReadNumForCurrentSplit = remainReadSize
        }
        else {
          lenForCurrentSplit -= readSize
          haveReadNumForCurrentSplit += readSize
        }

        totalReadNum += readSize
        writer.write(readBuffer, 0, readSize)

        if (readSize > 0)
          readSize = reader.read(readBuffer)
        else
          readSize = 0
      }

      if (lenForCurrentSplit <= rangePerNode && totalReadNum - startIndForCurrentSplit > 0) {
        currentSplit.add(new FileSegment(conf.getFileName(fileName), startIndForCurrentSplit, (totalReadNum - startIndForCurrentSplit).toInt, 0))
      }

      val tmp = outLocStart
      outLocStart += totalReadNum
      fileName2MergeStartMapper.put(conf.getFileName(fileName), (tmp, outLocStart))
    }
    writer.close()

    if (!currentSplit.isEmpty) {
      inputSplits.add(new InputSplit(outLocStart - (rangePerNode - lenForCurrentSplit), (rangePerNode - lenForCurrentSplit), currentSplit.asScala.toArray))
      currentSplit.clear()
    }
    val splits = inputSplits.asScala.toArray

    checkSplit(splits, lengths)

    (fileName2MergeStartMapper, splits)
  }

  def checkSplit(inputSplits: Array[InputSplit], lengths: Map[String, Long]): Unit = {
    val len = inputSplits.length

    val fileLength = new mutable.HashMap[String, (Long, Long)]
    var i = 0
    while (i < len) {
      val currentSplit = inputSplits(i)


      var lengthForCurrentSplit = 0

      var j = 0
      while (j < currentSplit.segments.length) {
        val currentFileSegment = currentSplit.segments(j)

        if (currentFileSegment.start + currentFileSegment.len + currentFileSegment.remainNum != lengths(currentFileSegment.fileName))
          throw new Exception(f"file Segment length wrong.\n segment = $currentFileSegment  \n " +
            f"ought to be $lengths")

        lengthForCurrentSplit += currentFileSegment.len

        fileLength.get(currentFileSegment.fileName) match {
          case None => {
            if (currentFileSegment.start != 0)
              throw new Exception("File is not start at 0")
            fileLength.put(currentFileSegment.fileName, (currentFileSegment.start, currentFileSegment.len))
          }
          case Some((start, len)) => {
            if (currentFileSegment.start != len)
              throw new Exception("file has been cut")
            fileLength.put(currentFileSegment.fileName, (start, len + currentFileSegment.len))
          }
        }
        j += 1
      }
      if (lengthForCurrentSplit /*将所有segement相加的结果*/ != currentSplit.mergeFileLen /*split的长度*/ )
        throw new Exception("split length wrong")

      i += 1
    }

    fileLength.foreach { case (fileName, (start, len /*将segment的length*/ )) =>
      if (len != lengths(fileName))
        throw new Exception(s"file length wrong for $fileName")
    }
  }

  def apply(path: String, conf: GctConf): UnifiedMap[String, (Long, Long)] = {

    val names = ListFiles(path)
    val outputPath = conf.mergedFilePath
    val writer = FSWriterFactory(outputPath)
    writer.open(outputPath)
    val readBuffer = new Array[Byte](conf.fsWriteBufferSize)
    var outLocStart: Long = 0L
    val fileName2MergeStartMapper = UnifiedMap.newMap[String, (Long, Long)](names.size)

    names.foreach { fileName =>

      val reader = FSReaderFactory(fileName)
      var currentInd: Long = 0
      var readSize = reader.read(readBuffer)

      while (readSize > 0) {
        currentInd += readSize
        writer.write(readBuffer, 0, readSize)
        if (readSize > 0)
          readSize = reader.read(readBuffer)
        else
          readSize = 0
      }
      val tmp: Long = outLocStart
      outLocStart += currentInd
      fileName2MergeStartMapper.put(conf.getFileName(fileName), (tmp, outLocStart))
    }
    writer.close()

    fileName2MergeStartMapper
  }

}
