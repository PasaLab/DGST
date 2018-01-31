package gct.rdd.top.trie.info

import org.eclipse.collections.impl.list.mutable.FastList

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * Created by Guo on 2016/10/24.
 */
class WrappedBuffer[T <: scala.AnyRef : ClassTag] extends Serializable {
  var buffer = FastList.newList[T]()
  var arr: Array[T] = null

  def addIndex(input: T) = buffer.add(input)

  def addIndex(input: java.util.Collection[_ <: T]) = buffer.addAll(input)

  def merge(that: WrappedBuffer[T]): Unit = {
    buffer.addAll(that.buffer)
  }

  def value(): Array[T] = {
    if (buffer.size() > 0 && arr == null)
      toArray()
    else
      arr
  }

  def toArray(): Array[T] = {
    if (buffer.size != 0) {
      if (arr == null) {
        arr = buffer.asScala.toArray
      }
      buffer.clear()
    }
    arr
  }

  def size(): Int = {
    var res = 0
    if (buffer != null)
      res += buffer.size()
    if (arr != null)
      res += arr.size
    res
  }

}
