package gct.rdd.input

import gct.rdd.GctConf
import util.file.FSReaderFactory

/**
 * Input Split which is the parallel computing unit.
 * @param mergeFileStart
 * @param mergeFileLen
 * @param segments
 */
case class InputSplit(mergeFileStart: Long, mergeFileLen: Int,
                      segments: Array[FileSegment]) {
  var content: Array[Byte] = _
  var extraLen: Int = _

  //Read the input split content from the merged string
  def readContent(conf: GctConf): InputSplit = {

    //Get extra tail
    extraLen = math.min(segments.last.remainNum, conf.fsExtraLen).toInt
    if (extraLen == 0)
      println("File ends")

    var actualLen = mergeFileLen + extraLen
    content = new Array[Byte](actualLen)

    val reader = FSReaderFactory(conf.mergedFilePath)
    val buffer = new Array[Byte](conf.fsReadBufferSize)
    reader.seek(mergeFileStart)
    var currentReadSize = 0
    while (actualLen > 0) {
      val readSize = reader.read(buffer)
      val copySize = math.min(readSize, actualLen)
      try {
        if (copySize > 0)
          System.arraycopy(buffer, 0, content, currentReadSize, copySize)
      }
      catch {
        case _ => {
          println("array copy exception")
        }
      }
      currentReadSize += copySize
      actualLen -= readSize
    }

    this
  }
}
