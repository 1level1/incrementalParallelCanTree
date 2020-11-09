package levko.cantree.utils

import org.apache.spark.Partitioner

class CanTreePartitioner(numberOfPartitioner: Int) extends Partitioner {

  override def numPartitions: Int = numberOfPartitioner
  private val r = scala.util.Random
  r.setSeed(1000L)
  override def getPartition(key: Any): Int = {
    return r.nextInt(numPartitions)
//    Math.abs(key.asInstanceOf[String].hashCode()% numPartitions)
  }

  // Java equals method to let Spark compare our Partitioner objects

  override def equals(other: Any): Boolean = other match {
    case partitioner: CanTreePartitioner =>
      partitioner.numPartitions == numPartitions
    case _ =>
      false
  }

}
