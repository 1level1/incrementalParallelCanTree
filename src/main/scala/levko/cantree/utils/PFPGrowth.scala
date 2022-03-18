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


import levko.cantree.utils.PFPGrowth.FreqItemset
import levko.cantree.utils.cantreeutils.log

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

import java.time.LocalDateTime
//import org.apache.spark.mllib.fpm.FPGrowth._
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel


class PFPGrowth  (
                                private var minSupport: Double,
                                private var numPartitions: Int,
                                private var partitioner : HashPartitioner,
                                private var collectStatistics: Boolean) extends Logging with Serializable {

  def this() = this(0.3, -1,new HashPartitioner(1),true)

  def setMinSupport(minSupport: Double): this.type = {
    require(minSupport >= 0.0 && minSupport <= 1.0,
      s"Minimal support level must be in range [0, 1] but got ${minSupport}")
    this.minSupport = minSupport
    this
  }

  def setNumPartitions(numPartitions: Int): this.type = {
    require(numPartitions > 0,
      s"Number of partitions must be positive but got ${numPartitions}")
    this.numPartitions = numPartitions
    this
  }
  def setPartitioner(partitioner: HashPartitioner): this.type = {
    require(partitioner != None,
      s"Partitioner must be initiated but got ${partitioner}")
    this.partitioner = partitioner
    this
  }
  def setCollectStatistics(status: Boolean) : this.type = {
    this.collectStatistics = status
    this
  }

  def getPartition(): HashPartitioner = {
    this.partitioner
  }
  def getMinSupport() : Double = {
    this.minSupport
  }

  //  /**
  //   * Java-friendly version of `run`.
  //   */
  //  def run[Item, Basket <: JavaIterable[Item]](data: JavaRDD[Basket]): FPGrowthModel[Item] = {
  ////    implicit val tag = fakeClassTag[Item]
  //    run(data.rdd.map(_.asScala.toArray))
  //  }


  def run[Item: ClassTag](data: RDD[Array[Item]],iter: Int): RDD[FreqItemset[Item]] = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }
    val count = data.count()
    val minCount = math.ceil(minSupport * count).toLong
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val freqItemsCount = genFreqItems(data, minCount, partitioner)
    val trees = generatePFPTrees(data,freqItemsCount.map(_._1),partitioner)

    val freqItemsets = genFreqItemsetsFromTrees(trees,minCount,freqItemsCount.map(_._1),partitioner);
    val fisCount =   freqItemsets.count()
    if (collectStatistics) {
      val treeSize = trees.map(part => part._2.nodesNum)
      treeSize.cache()
      val maxTreeSize = treeSize.max()
      val minTreeSize = treeSize.min()
      val meanTreeSize = treeSize.mean()
      log.info(LocalDateTime.now + "-iterateAndReportPFPGrowth- Found " + fisCount + " at iteration number " + iter + " (" + minTreeSize + "," + meanTreeSize + "," + maxTreeSize + ") Partitions: " + partitioner.numPartitions)
    } else
      log.info(LocalDateTime.now + "-iterateAndReportPFPGrowth- Found " + fisCount + " at iteration number " + iter + " Partitions: " + partitioner.numPartitions)
//    val freqItemsets = genFreqItemsets(data, minCount, freqItemsCount.map(_._1), partitioner)
    freqItemsets
  }

  private def genFreqItems[Item: ClassTag](
                                            data: RDD[Array[Item]],
                                            minCount: Long,
                                            partitioner: Partitioner): Array[(Item, Long)] = {
    data.flatMap { t =>
      val uniq = t.toSet
      if (t.length != uniq.size) {
        throw new SparkException(s"Items in a transaction must be unique but got ${t.toSeq}.")
      }
      t
    }.map(v => (v, 1L))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minCount)
      .collect()
      .sortBy(-_._2)
  }

  /**
   * Generate frequent itemsets by building FP-Trees, the extraction is done on each partition.
   * @param data transactions
   * @param minCount minimum count for frequent itemsets
   * @param freqItems frequent items
   * @param partitioner partitioner used to distribute transactions
   * @return an RDD of (frequent itemset, count)
   */
  private def generatePFPTrees[Item:ClassTag](
                         data: RDD[Array[Item]],
                         freqItems: Array[Item],
                         partitioner: Partitioner
                                             ) : RDD[(Int, PFPTree[Int])] = {
    val itemToRank = freqItems.zipWithIndex.toMap
    data.flatMap { transaction =>
      genCondTransactions(transaction, itemToRank, partitioner)
    }.aggregateByKey(new PFPTree[Int], partitioner.numPartitions)(
      (tree, transaction) => tree.add(transaction, 1L),
      (tree1, tree2) => tree1.merge(tree2))
  }

  private def genFreqItemsetsFromTrees[Item: ClassTag](
                                               trees: RDD[(Int, PFPTree[Int])],
                                               minCount: Long,
                                               freqItems: Array[Item],
                                               partitioner: Partitioner): RDD[FreqItemset[Item]] = {
    trees.flatMap { case (part, tree) =>
      tree.extract(minCount, x => partitioner.getPartition(x) == part)
    }.map { case (ranks, count) =>
      new FreqItemset(ranks.map(i => freqItems(i)).toArray, count)
    }
  }

  private def genFreqItemsets[Item: ClassTag](
                                               data: RDD[Array[Item]],
                                               minCount: Long,
                                               freqItems: Array[Item],
                                               partitioner: Partitioner): RDD[FreqItemset[Item]] = {
    val itemToRank = freqItems.zipWithIndex.toMap
    data.flatMap { transaction =>
      genCondTransactions(transaction, itemToRank, partitioner)
    }.aggregateByKey(new PFPTree[Int], partitioner.numPartitions)(
      (tree, transaction) => tree.add(transaction, 1L),
      (tree1, tree2) => tree1.merge(tree2))
      .flatMap { case (part, tree) =>
        tree.extract(minCount, x => partitioner.getPartition(x) == part)
      }.map { case (ranks, count) =>
      new FreqItemset(ranks.map(i => freqItems(i)).toArray, count)
    }
  }

  private def genCondTransactions[Item: ClassTag](
                                                   transaction: Array[Item],
                                                   itemToRank: Map[Item, Int],
                                                   partitioner: Partitioner): mutable.Map[Int, Array[Int]] = {
    val output = mutable.Map.empty[Int, Array[Int]]
    // Filter the basket by frequent items pattern and sort their ranks.
    val filtered = transaction.flatMap(itemToRank.get)
    ju.Arrays.sort(filtered)
    val n = filtered.length
    var i = n - 1
    while (i >= 0) {
      val item = filtered(i)
      val part = partitioner.getPartition(item)
      if (!output.contains(part)) {
        output(part) = filtered.slice(0, i + 1)
      }
      i -= 1
    }
    output
  }
}

object PFPGrowth {
  class FreqItemset[Item] (
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
