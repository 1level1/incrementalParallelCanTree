/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package levko.cantree.utils

import java.{util => ju}
import java.lang.{Iterable => JavaIterable}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.{HashPartitioner, Partitioner, SparkContext, SparkException}
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.internal.Logging
import levko.cantree.utils.CanTreeFPGrowth._
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel



/**
 * A parallel FP-growth algorithm to mine frequent itemsets. The algorithm is described in
 * <a href="https://doi.org/10.1145/1454008.1454027">Li et al., PFP: Parallel FP-Growth for Query
 * Recommendation</a>. PFP distributes computation in such a way that each worker executes an
 * independent group of mining tasks. The FP-Growth algorithm is described in
 * <a href="https://doi.org/10.1145/335191.335372">Han et al., Mining frequent patterns without
 * candidate generation</a>.
 *
 * @param minSupport the minimal support level of the frequent pattern, any pattern that appears
 *                   more than (minSupport * size-of-the-dataset) times will be output
 * @param numPartitions number of partitions used by parallel FP-growth
 *
 * @see <a href="http://en.wikipedia.org/wiki/Association_rule_learning">
 * Association rule learning (Wikipedia)</a>
 *
 */
class CanTreeFPGrowth(
    private var minSupport: Double,
    private var numPartitions: Int,
    private var totalItems : Long = 0L ) extends Logging with Serializable {

  /**
   * Constructs a default instance with default parameters {minSupport: `0.3`, numPartitions: same
   * as the input data}.
   *
   */
  def this() = this(0.3, -1)

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

  /**
   * Sets the number of partitions used by parallel FP-growth (default: same as input data).
   *
   */
  def setNumPartitions(numPartitions: Int): this.type = {
    require(numPartitions > 0,
      s"Number of partitions must be positive but got ${numPartitions}")
    this.numPartitions = numPartitions
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
   * @param data input data set, each element contains a transaction
   * @return an [[FPGrowthModel]]
   *
   */
  def run[Item: ClassTag](data: RDD[Array[Item]],
                          sorterFunction: (Item,Item) => Boolean): RDD[FreqItemset[Item]] = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }
    val count = data.count()
    totalItems+=count
    val minCount = math.ceil(minSupport * totalItems).toLong
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
//    val freqItemsCount = genFreqItems(data, minCount, partitioner)
    val freqItemsets = genFreqItemsets(data, minCount, sorterFunction, partitioner)
//    val itemSupport = freqItemsCount.map {
//      case (item, cnt) => item -> cnt.toDouble / count
//    }.toMap
    freqItemsets
  }

//  /**
//   * Java-friendly version of `run`.
//   */
//  def run[Item, Basket <: JavaIterable[Item]](data: JavaRDD[Basket]): FPGrowthModel[Item] = {
////    implicit val tag = fakeClassTag[Item]
//    run(data.rdd.map(_.asScala.toArray))
//  }

  /**
   * Generate frequent itemsets by building FP-Trees, the extraction is done on each partition.
   * @param data transactions
   * @param minCount minimum count for frequent itemsets
   * @param sorterFunction frequent items
   * @param partitioner partitioner used to distribute transactions
   * @return an RDD of (frequent itemset, count)
   */
  private def genFreqItemsets[Item: ClassTag](
      data: RDD[Array[Item]],
      minCount: Long,
//      freqItems: Array[Item],
      sorterFunction: (Item,Item) => Boolean,
      partitioner: Partitioner): RDD[FreqItemset[Item]] = {
//    val itemToRank = freqItems.zipWithIndex.toMap
    data.flatMap { transaction =>
      genCondTransactions(transaction, sorterFunction, partitioner)
    }.aggregateByKey(new CanTreeV1[Item], partitioner.numPartitions)(
      (tree, transaction) => {
        tree.add(transaction, 1L)
      },
      (tree1, tree2) => {
        tree1.merge(tree2)
      }).flatMap { case (part, tree) =>
      tree.extract(minCount, x => partitioner.getPartition(x) == part)
    }.map { case (ranks, count) =>
//      new FreqItemset(ranks.map(i => freqItems(i)).toArray, count)
      new FreqItemset(ranks.toArray, count)
    }
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
//      itemToRank: Map[Item, Int],
      sorterFunction: (Item,Item) => Boolean,
      partitioner: Partitioner): mutable.Map[Int, Array[Item]] = {
    val output = mutable.Map.empty[Int, Array[Item]]
    // Filter the basket by frequent items pattern and sort their ranks.
    val filtered = transaction.sortWith(sorterFunction)
//    ju.Arrays.sort(filtered)
    val n = filtered.length
    var i = n - 1
    while (i >= 0) {
      val item = filtered(i)
      val part = partitioner.getPartition(item)
      if (!output.contains(part)) {
        output(part) = filtered.slice(0, i + 1).reverse
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
