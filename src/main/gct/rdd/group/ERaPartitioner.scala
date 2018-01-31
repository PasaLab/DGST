package gct.rdd.group

import org.apache.spark.Partitioner
import org.eclipse.collections.api.map.MutableMap

/**
 * ERa partitioner using in ERa algorithm
 */
class ERaPartitioner(parts: Int, rootMap: MutableMap[String, Int]) extends Partitioner {
  override def numPartitions: Int = parts

  override def getPartition(key: Any): Int = {
    val x = rootMap.get(key.asInstanceOf[String])
    if (x != null)
      x % parts
    //Protection, no meaning
    else if (parts > rootMap.size())
      (rootMap.size() + 1) % parts
    else
      1 % parts
  }
}
