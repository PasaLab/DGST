package gct.rdd.group

import java.util.Comparator

import org.eclipse.collections.api.map.MutableMap
import org.eclipse.collections.impl.list.mutable.FastList
import org.eclipse.collections.impl.map.mutable.UnifiedMap

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
 * Different grouping strategies, each of which corresponds to a task allocation algorithm.
 * 'ff' strategy is used in ERa algotihm.
 * 'bfhg' strategy is used in our DGST algorithm.
 */
object Grouping {

  def apply(subTreeRoots: FastList[(String, Long)], memBudget: Int, method: String, partition: Int): MutableMap[String, Int] = {
    method.toLowerCase match {
      case "hg" => myHeapGreedy(subTreeRoots, memBudget, partition)
      case "bfhg" => heapGreedy(subTreeRoots, memBudget, partition)
      case "wf" => worstFitDesc(subTreeRoots, memBudget)
      case "bf" => bestFitDesc(subTreeRoots, memBudget)
      case "ff" => firstFitDesc(subTreeRoots, memBudget)
    }
  }

  /**
   * Worst Fit Algorithm
   * @param subTreeRoots
   * @param memBudget
   * @return
   */
  def worstFitDesc(subTreeRoots: FastList[(String, Long)], memBudget: Int): MutableMap[String, Int] = {

    val sortedSubTreeRoots = subTreeRoots.clone()
    sortedSubTreeRoots.sortThis(new PrefixCountDecreaseComparator)
    val res: MutableMap[String, Int] = UnifiedMap.newMap[String, Int](subTreeRoots.size())

    case class HeapNode(group: Int, var load: Long)

    val heap = FastList.newListWith[HeapNode](new HeapNode(0, 0))
    def lc(i: Int) = 2 * i + 1
    def rc(i: Int) = 2 * i + 2
    def swap(i: Int, j: Int) = {
      val tmp = heap.get(i)
      heap.set(i, heap.get(j))
      heap.set(j, tmp)
    }
    @tailrec
    def adjustTopDown(i: Int): Unit = {
      if (lc(i) >= heap.size()) return
      var next = if (heap.get(i).load > heap.get(lc(i)).load) lc(i) else i
      next = if (rc(i) < heap.size() && heap.get(next).load > heap.get(rc(i)).load) rc(i) else next
      if (next != i) {
        swap(i, next)
        adjustTopDown(next)
      }
    }
    def adjectBottonUp(i: Int): Unit = {
      var ind = i
      while (ind > 0) {
        if (heap.get(ind).load < heap.get(ind / 2).load)
          swap(ind, ind / 2)
        else
          ind = 0

        ind /= 2
      }
    }

    var i = 0
    while (i < sortedSubTreeRoots.size()) {
      val currentRoot = sortedSubTreeRoots.get(i)
      val tmp = heap.get(0)
      if (tmp.load + currentRoot._2 > memBudget) {
        val newGroupInd = heap.size
        res.put(currentRoot._1, newGroupInd)
        heap.add(new HeapNode(newGroupInd, currentRoot._2))
        adjectBottonUp(newGroupInd)
      }
      else {
        heap.get(0).load = tmp.load + currentRoot._2
        res.put(currentRoot._1, heap.get(0).group)
        adjustTopDown(0)
      }
      i += 1
    }

    res

  }

  /**
   * First Fit algorithm
   * @param subTreeRoots
   * @param memBudget
   * @return
   */
  def firstFitDesc(subTreeRoots: FastList[(String, Long)], memBudget: Int): MutableMap[String, Int] = {
    val xx = subTreeRoots.clone()
    xx.sortThis(new PrefixCountDecreaseComparator)
    val res: MutableMap[String, Int] = UnifiedMap.newMap[String, Int](subTreeRoots.size())

    var groupInd = 0
    while (xx.notEmpty()) {
      var tmp = xx.getFirst
      xx.remove(0)
      var capacity = memBudget - tmp._2
      res.put(tmp._1, groupInd)
      var i = 0
      while (i < xx.size()) {
        tmp = xx.get(i)
        if (capacity - tmp._2 >= 0) {
          capacity -= tmp._2
          res.put(tmp._1, groupInd)
          xx.remove(i)
          i -= 1
        }
        i += 1
      }
      groupInd += 1
    }
    res
  }

  /**
   * Hybrid grouping strategy by combining the Bin-Packing solution and Number-Partitioning solution
   * @param subTreeRoots
   * @param memBudget
   * @param partition
   * @return
   */
  def heapGreedy(subTreeRoots: FastList[(String, Long)], memBudget: Int, partition: Int): MutableMap[String, Int] = {

    val minBuckets = bestFitDesc(subTreeRoots, memBudget).values.asScala.max

    val buckets = (minBuckets / partition + 1) * partition

    myHeapGreedy(subTreeRoots, memBudget, buckets)
  }

  /**
   * Best Fit Algorithm
   * @param subTreeRoots
   * @param memBudget
   * @return
   */
  def bestFitDesc(subTreeRoots: FastList[(String, Long)], memBudget: Int): MutableMap[String, Int] = {

    val sortedSubTreeRoots = subTreeRoots.clone()
    sortedSubTreeRoots.sortThis(new PrefixCountDecreaseComparator)
    val res: MutableMap[String, Int] = UnifiedMap.newMap[String, Int](subTreeRoots.size())

    case class HeapNode(group: Int, var load: Long)

    val heap = FastList.newListWith[HeapNode](new HeapNode(0, 0))
    def lc(i: Int) = 2 * i + 1
    def rc(i: Int) = 2 * i + 2
    def swap(i: Int, j: Int) = {
      val tmp = heap.get(i)
      heap.set(i, heap.get(j))
      heap.set(j, tmp)
    }

    def findBestBucket(load: Long, currentNode: Int): Int = {
      if (heap.get(currentNode).load + load <= memBudget)
        return currentNode
      else {
        val tmpLc = if (lc(currentNode) < heap.size()) findBestBucket(load, lc(currentNode)) else heap.size()
        val tmpRc = if (rc(currentNode) < heap.size()) findBestBucket(load, rc(currentNode)) else heap.size()

        if (tmpLc == tmpRc)
          return heap.size()
        else {
          if (tmpLc == heap.size())
            return tmpRc
          else if (tmpRc == heap.size() || heap.get(tmpLc).load > heap.get(tmpRc).load)
            return tmpLc
          else
            return tmpRc
        }
      }

    }

    def adjectBottonUp(i: Int): Unit = {
      var ind = i
      while (ind > 0) {
        if (heap.get(ind).load > heap.get(ind / 2).load)
          swap(ind, ind / 2)
        else
          ind = 0

        ind /= 2
      }
    }

    var i = 0
    while (i < sortedSubTreeRoots.size()) {
      val currentRoot = sortedSubTreeRoots.get(i)
      val fit = findBestBucket(currentRoot._2, 0)
      if (fit >= heap.size()) {
        val newGroupInd = heap.size
        res.put(currentRoot._1, newGroupInd)
        heap.add(new HeapNode(newGroupInd, currentRoot._2))
        adjectBottonUp(newGroupInd)
      }
      else {
        heap.get(fit).load += currentRoot._2
        res.put(currentRoot._1, heap.get(fit).group)
        adjectBottonUp(fit)
      }
      i += 1
    }
    res

  }

  def myHeapGreedy(subTreeRoots: FastList[(String, Long)], memBudget: Int, buckets: Int): MutableMap[String, Int] = {

    val sortedSubTreeRoots = subTreeRoots.clone()
    sortedSubTreeRoots.sortThis(new PrefixCountDecreaseComparator)
    val res: MutableMap[String, Int] = UnifiedMap.newMap[String, Int](subTreeRoots.size())

    case class HeapNode(group: Int, var load: Long)

    val heap = FastList.newList[HeapNode](buckets)

    var i = 0
    while (i < buckets) {
      heap.add(new HeapNode(i, 0))
      i += 1
    }

    def lc(i: Int) = 2 * i + 1
    def rc(i: Int) = 2 * i + 2
    def swap(i: Int, j: Int) = {
      val tmp = heap.get(i)
      heap.set(i, heap.get(j))
      heap.set(j, tmp)
    }
    @tailrec
    def adjustTopDown(i: Int): Unit = {
      if (lc(i) >= heap.size()) return
      var next = if (heap.get(i).load > heap.get(lc(i)).load) lc(i) else i
      next = if (rc(i) < heap.size() && heap.get(next).load > heap.get(rc(i)).load) rc(i) else next
      if (next != i) {
        swap(i, next)
        adjustTopDown(next)
      }
    }

    i = 0
    while (i < sortedSubTreeRoots.size()) {
      val currentRoot = sortedSubTreeRoots.get(i)
      val tmp = heap.get(0)
      heap.get(0).load = tmp.load + currentRoot._2
      res.put(currentRoot._1, heap.get(0).group)
      adjustTopDown(0)
      i += 1
    }

    res
  }

  def makeIter(groupingMap: MutableMap[String, Int], partitions: Int): Array[FastList[String]] = {
    val totalIters = (groupingMap.values().asScala.max) / partitions + 1
    val res = new Array[FastList[String]](totalIters)
    var i = 0
    while (i < totalIters) {
      res(i) = FastList.newList[String]
      i += 1
    }

    groupingMap.entrySet().asScala.foreach { entry =>
      val bucket = entry.getValue / partitions
      res(bucket).add(entry.getKey)
    }
    res
  }

  def buckets2Partition(i: Int, partitions: Int): Int = {
    i % partitions
  }

  def buckets2Partition(i: Int, mapper: Map[Int, Int], partitions: Int): Int = {
    (mapper(i)) % partitions
  }

  /**
   * Evaluate performance of grouping strategy
   * @param input
   * @param groupingRes
   * @param budget
   * @param partition
   */
  def review(input: FastList[(String, Long)], groupingRes: MutableMap[String, Int], budget: Int, partition: Int): Unit = {
    val oriBuckets = groupingRes.values().asScala.max + 1
    val oriLoad = new Array[Long](oriBuckets)
    val totalIters = groupingRes.values().asScala.max / partition + 1
    println("iters:" + totalIters)

    val avgForIter = new Array[Long](totalIters)

    input.asScala.foreach { case (root, value) =>
      oriLoad(groupingRes.get(root)) += value
      avgForIter(groupingRes.get(root) / partition) += value
    }
    for (i <- 0 until totalIters) {
      avgForIter(i) /= partition
      var std = 0.0
      var j = 0
      while (j < partition && i * partition + j < oriBuckets) {
        std += math.pow(oriLoad(i * partition + j) - avgForIter(i), 2)
        j += 1
      }
      std = math.sqrt(std / partition)
      println("iter " + i + ": mean: " + avgForIter(i) + "  sigma:" + std)
    }
  }

  class PrefixCountDecreaseComparator extends Comparator[(String, Long)] {
    override def compare(o1: (String, Long), o2: (String, Long)): Int = {
      if (o2._2 - o1._2 < 0) return -1
      else if (o2._2 - o1._2 > 0) return 1
      else return 0
    }
  }

}
