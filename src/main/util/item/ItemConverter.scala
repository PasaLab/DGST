package util.item

/**
 * Item Converter
 */
object ItemConverter {
  /**
   * end included
   * @param input
   * @param start
   * @param end
   * @param bitsPerItem
   * @return
   */
  def arrayByte2Int(input: Array[Byte], start: Int, end: Long /*主要是包含remainNum*/ , bitsPerItem: Int): Int = {
    var res: Int = 0
    var i: Int = start
    val actualEnd = math.min(start + 32 / bitsPerItem, end).toInt
    while (i < actualEnd) {
      res = (res << bitsPerItem) | input(i)
      i += 1
    }
    var restLength: Int = 32 / bitsPerItem - (actualEnd - start)
    if (restLength > 0) {
      var TERMINAL: Int = 0
      var tmp: Int = 0
      while (tmp < bitsPerItem) {
        TERMINAL = (TERMINAL << 1) | 1
        tmp += 1
      }
      res = res << bitsPerItem | TERMINAL
      restLength -= 1
    }
    val alignBit: Int = 32 % bitsPerItem + restLength * bitsPerItem
    res <<= alignBit
    return res
  }
}
