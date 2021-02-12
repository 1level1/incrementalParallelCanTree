package levko.cantree.utils
import levko.cantree.utils.CanTreeFPGrowth.FreqItemset
import levko.cantree.utils.cantreeutils._
import org.apache.spark.Partitioner

import scala.collection.mutable
import scala.collection.mutable.Set
import scala.reflect.ClassTag
class IncMiningPFP[T:ClassTag](var canTree : CanTreeV1[T]) extends Serializable {
//  The improvement in song2017 (IncMiningPFP) works as follows:
//    A. First proposed a general structure that doesn`t support new items
//  For baseDB:
//    1) Read DB and create a base-order of frequencies
//  2) Create a group-list of the items (they choose a method to group frequency based items, not random - section 5.4.2)
//  3) run PFP and save results in shards, per group.
//  for every ∆DB:
//    5) Extract participating items, save in new frequency list, without changing order.
//  6) Update the trees in affected groups.
//  7) Create a new IncFPTree, which will have only items extracted from 1. This way the tree is expected to be much smaller.
//  8) Run nre mining only on 3 and extract FIS
//  9) Join 4 to already cached FIS from previous step, and update cache for each group.
//
//  B. To support new items, another improvement proposed and works as follows:
//    From my understanding, new items that are added to some group, or a new group, must come first in the ordering, to not break the tree structure.
//  The new item, and the transaction where it appears, added to the tree where the new item is the 1st item in the tree (right under the root).


  var freqItemsetSet : Set[FreqItemset[T]] = Set.empty
  var isMined : Boolean = false
  def addTransaction(t: Iterable[T], count: Long = 1L): Unit = {
    canTree.add(t,count)
  }
  def merge(other:IncMiningPFP[T]): IncMiningPFP[T] = {
    canTree.merge(other.canTree)
    this
  }
  def merge(other:CanTreeV1[T]): IncMiningPFP[T] = {
    canTree.merge(other)
    isMined=false
    this
  }

  def minedStatus() : Boolean = {
    isMined
  }

  def setMinedStatus(mine :Boolean) : Unit = {
    isMined=mine
  }

  def calcFreqItems(freqItems: List[T],minCount : Long,validateSuffix: T => Boolean = _ => true) : Set[FreqItemset[T]] = {
    if (isMined)
      return freqItemsetSet
    val incTree = getIncTree(freqItems)
    val freqItemsets = incTree.extract(minCount,validateSuffix)
    freqItemsets.foreach { case (ranks, count) =>
      freqItemsetSet.add(new FreqItemset[T](ranks.toArray, count))
    }
    isMined = true
    freqItemsetSet
  }
//  def getFrequentItems (minSupport:Long,  incCanTree:CanTreeV1[T],transactions: mutable.Map[Int, Array[T]]) :
  def getIncTree (freqItems: List[T])  : CanTreeV1[T] = {
    val tree = new CanTreeV1[T]
    val transactions : Iterator[(List[T], Long)] = canTree.transactions(freqItems)
    transactions.foreach{ case (t, c) =>
      tree.add(t, c)
    }
    tree
  }
}
