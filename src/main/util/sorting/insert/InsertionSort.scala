package util.sorting.insert

import util.sorting.SortableArray

/**
 * Insert Sort Algorithm
 */
object InsertionSort {

  def apply(input: SortableArray): Unit = {
    apply(input, 0)
  }

  def apply(input: SortableArray, prefixLen: Int): Unit = {
    val start = input.start()
    val end = input.end()
    apply(input, start, end, prefixLen)

  }

  def apply(input: SortableArray, start: Int, end: Int, prefixLen: Int) = {

    var i = start + 1
    var j = i
    while (i <= end) {
      j = i
      while (j > start && (input.lessThan(j, j - 1, prefixLen) > 0)) {
        input.swap(j, j - 1)
        j -= 1
      }
      i += 1
    }
  }


}
