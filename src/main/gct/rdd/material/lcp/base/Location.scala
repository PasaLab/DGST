package gct.rdd.material.lcp.base

/**
 * Suffix location information
 * @param fileName
 * @param startInd
 * @param suffixLength  = fileEnd - startInd. Means that prefix included
 * @param prefixLen prefix length
 */
case class Location(fileName: String, startInd: Long, suffixLength: Long, var prefixLen: Int) {
  def this(input: (String, Long, Long), preLen: Int) {
    this(input._1, input._2, input._3, preLen)
  }

  def this(fileName: String, startInd: Long, suffixLength: Long) {
    this(fileName, startInd, suffixLength, 0)
  }

}
