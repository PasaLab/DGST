package util.sorting

import gct.rdd.material.io.SharedBufferSortableArray
import util.sorting.insert.InsertionSort
import util.sorting.quick.NaiveQuickSort
import util.sorting.radix.RadixSort
import util.sorting.system.JavaSort

object Sorting {
  def apply(input: SharedBufferSortableArray, method: String): Unit = {
    method.toLowerCase match {
      case "ins" => InsertionSort(input)
      case "radix" => RadixSort(input, false)
      case "radix_ins" => RadixSort(input)
      case "naive_quick" => NaiveQuickSort(input, false)
      case _ => JavaSort(input)
    }
  }
}
