package util.sorting.quick

import util.sorting.SortableArray


object NaiveQuickSort {

  var CUTOFF = 15

  def apply(input: SortableArray): Unit = {
    quickSort(input, input.start(), input.end())
  }

  def apply(input: SortableArray, withCutoff: Boolean): Unit = {
    if (!withCutoff)
      CUTOFF = 1
    quickSort(input: SortableArray, input.start(), input.end())
  }

  /**
   * endInd not included
   * @param input
   * @param start
   * @param end
   */
  def quickSort(input: SortableArray, start: Int, end: Int): Unit = {
    if (end - start < CUTOFF)
      return

    val lessNum = oneStep(input, start, end)

    quickSort(input, start, start + lessNum - 1)
    quickSort(input, start + lessNum + 1, end)
  }

  def oneStep(input: SortableArray, start: Int, end: Int): Int = {
    val pivot = start

    var lessNum = 0
    var i = (start + 1)
    while (i <= end) {
      if (input.lessThan(pivot, i) < 0) {
        input.swap(lessNum + start + 1, i)
        lessNum += 1
      }
      i += 1
    }

    input.swap(lessNum + start, pivot)
    lessNum
  }

}
