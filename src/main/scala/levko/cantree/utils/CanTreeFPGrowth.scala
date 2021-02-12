package levko.cantree.utils


import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag
import org.apache.spark.{HashPartitioner, Partitioner, SparkException}
import org.apache.spark.internal.Logging
import levko.cantree.utils.CanTreeFPGrowth._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


class CanTreeFPGrowth(
    private var minSupport: Double,
    private var minMinSupport: Long,
    private var partitioner : HashPartitioner) extends Logging with Serializable {

  /**
   * Constructs a default instance with default parameters {minSupport: `0.3`, numPartitions: same
   * as the input data}.
   *
   */
  def this() = this(0.3, 0L,new HashPartitioner(1))

  /**
   * Sets the minimal support level (default: `0.3`).
   *
   */
  def setMinSupport(minSupport: Double): this.type = {
    require(minSupport >= 0.0 && minSupport <= 1.0,
      s"Minimal support level must be in range [0, 1] but got ${minSupport}")
    this.minSupport = minSupport
    this
  }

  def setMinMinSupport(minMinSupport: Long): this.type = {
    this.minMinSupport = minMinSupport
    this
  }

  /**
   * Sets the number of partitions used by parallel FP-growth (default: same as input data).
   *
   */
  def setPartitioner(partitioner: HashPartitioner): this.type = {
    require(partitioner != None,
      s"Partitioner must be initiated but got ${partitioner}")
    this.partitioner = partitioner
    this
  }

  def addDelta[Item: ClassTag](data: RDD[Array[Item]],
                               sorterFunction: (Item,Item) => Boolean) {
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning ("Input data is not cached.")
    }
  }

  /**
   * Computes an FP-Growth model that contains frequent itemsets.
   * @param canTrees generated CanTrees
   * @return an [[FPGrowthModel]]
   *
   */

  def run[Item: ClassTag](canTrees:RDD[(Int,CanTreeV1[Item])], minCount : Long): RDD[FreqItemset[Item]] =
  {
    if (canTrees.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }
    val freqItemsets = genFreqItemsets(canTrees, minCount,partitioner)
    freqItemsets
  }

//  /**
//   * Java-friendly version of `run`.
//   */
//  def run[Item, Basket <: JavaIterable[Item]](data: JavaRDD[Basket]): FPGrowthModel[Item] = {
////    implicit val tag = fakeClassTag[Item]
//    run(data.rdd.map(_.asScala.toArray))
//  }

  def genCanTrees[Item: ClassTag](data: RDD[Array[Item]],
                                          sorterFunction: (Item,Item) => Boolean, itemToRank : Map[Item,Long]  ): RDD[(Int,CanTreeV1[Item])] = {

//    val freqItemsCount = genFreqItems(data, minCount, partitioner)
//    val itemToRank = freqItems.zipWithIndex.toMap
    data.flatMap { transaction =>
      genCondTransactions(transaction, itemToRank,sorterFunction, partitioner)
    }.aggregateByKey(new CanTreeV1[Item], partitioner.numPartitions)(
      (tree, transaction) => {
      tree.add(transaction, 1L)
    },
      (tree1, tree2) => {
      tree1.merge(tree2)
    })
  }
  /**
   * Generate frequent itemsets by building FP-Trees, the extraction is done on each partition.
   * @param trees RDD of already prepared trees
   * @param minCount minimum count for frequent itemsets
   * @param partitioner partitioner used to distribute transactions
   * @return an RDD of (frequent itemset, count)
   */
  private def genFreqItemsets[Item: ClassTag](trees : RDD[(Int,CanTreeV1[Item])],
                                              minCount : Long,
                                              partitioner: Partitioner): RDD[FreqItemset[Item]] =
  {
    trees.flatMap { case (part, tree) =>
      tree.extract(minCount, x => partitioner.getPartition(x) == part)
    }.map { case (ranks, count) =>
      new FreqItemset(ranks.toArray, count)
    }
  }

  private def genFreqItems[Item: ClassTag](
                                            data: RDD[Array[Item]],
                                            partitioner: Partitioner): Array[(Item, Long)] = {
    data.flatMap { t =>
      val uniq = t.toSet
      if (t.length != uniq.size) {
        throw new SparkException(s"Items in a transaction must be unique but got ${t.toSeq}.")
      }
      t
    }.map(v => (v, 1L))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minMinSupport)
      .collect()
      .sortBy(-_._2)
  }
  /**
   * Generates conditional transactions.
   * @param transaction a transaction
   * @param sorterFunction map from item to their rank
   * @param partitioner partitioner used to distribute transactions
   * @return a map of (target partition, conditional transaction)
   */
  private def genCondTransactions[Item: ClassTag](
      transaction: Array[Item],
      itemToRank: Map[Item, Long],
      sorterFunction: (Item,Item) => Boolean,
      partitioner: Partitioner): mutable.Map[Int, Array[Item]] = {
    val output = mutable.Map.empty[Int, Array[Item]]
    // Filter the basket by frequent items pattern and sort their ranks.
    val filtered : mutable.ArrayBuffer[Item] =mutable.ArrayBuffer.empty
    transaction.iterator.foreach(i => if (itemToRank.contains(i)) filtered.append(i))
    val sorted = filtered.sortWith(sorterFunction).toArray
//    ju.Arrays.sort(filtered)
    val n = sorted.length
    var i = n - 1
    while (i >= 0) {
      val item = sorted(i)
      val part = partitioner.getPartition(item)
      if (!output.contains(part)) {
        output(part) = sorted.slice(0, i + 1)
      }
      i -= 1
    }
    output
  }
}

object CanTreeFPGrowth {

  /**
   * Frequent itemset.
   * @param items items in this itemset. Java users should call `FreqItemset.javaItems` instead.
   * @param freq frequency
   * @tparam Item item type
   *
   */
  class FreqItemset[Item]  (
      val items: Array[Item],
      val freq: Long) extends Serializable {

    /**
     * Returns items in a Java List.
     *
     */
    def javaItems: java.util.List[Item] = {
      items.toList.asJava
    }

    override def toString: String = {
      s"${items.mkString("{", ",", "}")}: $freq"
    }
  }
}
