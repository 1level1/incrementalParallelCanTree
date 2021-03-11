package levko.cantree.utils

import org.apache.spark.{HashPartitioner, Partitioner}

import scala.collection.mutable
import scala.reflect.ClassTag

class CanTreePartitioner[Item : ClassTag](numberOfPartitioner: Int) extends HashPartitioner(numberOfPartitioner) {

  override def numPartitions: Int = numberOfPartitioner
  private val r = scala.util.Random
  var numOfPartitions : Int = 0
  var itemsHash : mutable.HashMap[Item,Int] = mutable.HashMap.empty

  r.setSeed(1000L)

  def this(numberOfPartitioner: Int,itemsMap : List[Set[Item]]) {
    this(numberOfPartitioner)
    this.numOfPartitions = numberOfPartitioner
    var i = 0
    itemsMap.foreach( itemsGroup => {
      itemsGroup.foreach(item => {
          if (!this.itemsHash.contains(item))
            this.itemsHash(item) = i
        }
      )
      i+=1
    })
  }
  override def getPartition(key: Any): Int = {
    val item : Item = key.asInstanceOf[Item]
    if (this.itemsHash.contains(item)) {
      return this.itemsHash(item)
    }
    else {
      // Return next group
      return (this.numOfPartitions)
    }
  }

  // Java equals method to let Spark compare our Partitioner objects

  override def equals(other: Any): Boolean = other match {
    case partitioner: CanTreePartitioner[Item] =>
      partitioner.numPartitions == numPartitions
    case _ =>
      false
  }

}
