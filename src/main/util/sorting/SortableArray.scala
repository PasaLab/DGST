package util.sorting

abstract class SortableArray {
  def posLessThan(pivot: Int, i: Int, prefixLen: Int): Int

  def start(): Int

  //end can be fetch
  def end(): Int

  def lessThan(i: Int, j: Int): Int

  def swap(i: Int, j: Int)

  def lessThan(i: Int, j: Int, prefixLen: Int): Int

  def prefixTooLong(prefixLen: Int): Boolean
}
