package gct.rdd.input

/**
 * File segment information
 */
case class FileSegment(fileName: String, start: Long, len: Int, remainNum: Long = 0) {
  def getFileTotalLen = start + len + remainNum
}
