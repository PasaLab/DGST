package gct.rdd.material.lcp.base

import util.item.ItemConverter

/**
 * Suffix information
 */
class SharedBufferSuffix(input: Array[Byte], pos: Int, location: Location) extends Ordered[SharedBufferSuffix] with Serializable {
  private final val text: Array[Byte] = input
  private final val offset: Int = pos
  private final val fileName: String = loc.fileName
  val loc = location
  val unfinishedFlag = length + offset >= input.size

  def location3: (String, Long, Long) = (fileName, loc.startInd, loc.suffixLength)

  def location2: (String, Long) = (fileName, loc.startInd)

  def copyRange(start: Int, len: Int): Array[Byte] = {
    val actualLen = math.min(length - start, len).toInt

    val res = new Array[Byte](actualLen)
    System.arraycopy(text, offset + start, res, 0, actualLen)
    res
  }

  def getSegment(bitsPerItem: Int, prefixLen: Int): Int =
    ItemConverter.arrayByte2Int(text, prefixLen + offset, offset + length, bitsPerItem)

  /**
   * Suffix comparison
   */
  override def compare(that: SharedBufferSuffix): Int = {
    if (this eq that) return 0

    val n: Long = Math.min(this.length, that.length)

    try {
      var i: Int = 0
      while (i < n) {
        {
          if (this.index(i) < that.index(i)) return -1
          if (this.index(i) > that.index(i)) return +1
        }
        i += 1
      }
    }
    catch {
      case _ => {
        println("unfinished")
        return 0

      }
    }
    // Special solution in ERa, larger length is small
    val res = that.length - this.length
    return if (res > 0) 1 else if (res < 0) -1 else 0
  }

  def index(i: Int): Byte = if (i >= length) text((offset + length - 1).toInt) else text(offset + i)

  override def toString: String = {
    return text.toString
  }

  def toArrayAndTake(prefixLen: Int, take: Int): Array[Byte] = {
    val len = math.min(length - prefixLen, take).toInt
    val res = new Array[Byte](len)
    val start = offset + prefixLen
    System.arraycopy(text, start, res, 0, len)
    res
  }

  def length: Long = loc.suffixLength
}
