package util.sorting.radix

import util.sorting.SortableArray
import util.sorting.insert.InsertionSort

/**
 * Radix Sort Algorithm
 */
object RadixSort {

  var CUTOFF = 25

  def apply(input: SortableArray): Unit = {
    radixSort(input: SortableArray, input.start(), input.end(), 0)
  }

  def apply(input: SortableArray, withCutoff: Boolean): Unit = {
    if (!withCutoff)
      CUTOFF = 1
    radixSort(input: SortableArray, input.start(), input.end(), 0)
  }

  // end INCLUDED
  def radixSort(input: SortableArray, start: Int, end: Int, prefixLen: Int): Unit = {
    if (input.prefixTooLong(prefixLen))
      return

    if (end - start <= CUTOFF) {
      InsertionSort(input, start, end, prefixLen)
      return

    }
    val (less, greater) = oneStep(input, start, end, prefixLen)

    radixSort(input, start, less - 1, prefixLen)
    radixSort(input, greater + 1, end, prefixLen)
    //todo: should I do sth?
    radixSort(input, less, greater, prefixLen + 1)

  }

  def oneStep(input: SortableArray, start: Int, end: Int, prefixLen: Int): (Int, Int) = {
    var less = start
    var greater = end
    var i = start + 1
    var pivot = start
    while (i <= greater) {
      val comp = input.posLessThan(pivot, i, prefixLen)
      if (comp > 0) {
        input.swap(greater, i)
        greater -= 1
      }
      else if (comp < 0) {

        //keep pivot in right position
        if (pivot == less)
          pivot = i

        // Is pivot never equal to i ?
        else if (pivot == i)
          pivot = less

        input.swap(less, i)
        less += 1
        i += 1
      }
      else
        i += 1
    }
    (less, greater)
  }


}
