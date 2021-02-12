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
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.immutable

/**
 * FP-Tree data structure used in FP-Growth.
 * @tparam T item type
 */
class CanTreeV1[T] extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
  import CanTreeV1._

  val root: Node[T] = new Node(null)
  var nodesNum:Long = 0L
  private val summaries: mutable.Map[T, Summary[T]] = mutable.Map.empty

  /** Adds a transaction with count. */
  def add(t: Iterable[T], count: Long = 1L): CanTreeV1[T] = {
    require(count > 0)
    var curr = root
    curr.count += count
    t.foreach { item =>
      val summary = summaries.getOrElseUpdate(item, new Summary)
      summary.count += count
      val child = curr.children.getOrElseUpdate(item, {
        val newNode = new Node(curr)
        newNode.item = item
        summary.nodes += newNode
        nodesNum+=1
        newNode
      })
      child.count += count
      curr = child
    }
//    log.info("-1- NodesNum "+nodesNum)
    this
  }

  /** Merges another FP-Tree. */
  def merge(other: CanTreeV1[T]): CanTreeV1[T] = {
    other.transactions(List.empty).foreach { case (t, c) =>
      add(t, c)
    }
    this
  }

  /** Gets a subtree with the suffix. */
  private def project(suffix: T): CanTreeV1[T] = {
    val tree = new CanTreeV1[T]
    if (summaries.contains(suffix)) {
      val summary = summaries(suffix)
      summary.nodes.foreach { node =>
        var t = List.empty[T]
        var curr = node.parent
        while (!curr.isRoot) {
          t = curr.item :: t
          curr = curr.parent
        }
        tree.add(t, node.count)
      }
    }
    tree
  }

  /** Returns all transactions in an iterator. */
  def transactions(items : List[T] = List.empty): Iterator[(List[T], Long)] = getTransactions(root,items)

  /** Returns all transactions under this node. */
  private def getTransactions(node: Node[T],items : List[T]): Iterator[(List[T], Long)] = {
    var count = node.count
    node.children.iterator.flatMap { case (item, child) =>
      getTransactions(child,items).map { case (t, c) =>
        count -= c
        if (items.isEmpty)
          (item :: t, c)
        else {
          if (items.contains(item))
            (item :: t, c)
          else
            (t, c)
        }
      }
    } ++ {
      if (count > 0) {
        Iterator.single((Nil, count))
      } else {
        Iterator.empty
      }
    }
  }

  /** Extracts all patterns with valid suffix and minimum count. */
  def extract(
      minCount: Long,
      validateSuffix: T => Boolean = _ => true): Iterator[(List[T], Long)] = {
    summaries.iterator.flatMap { case (item, summary) =>
      if (validateSuffix(item) && summary.count >= minCount) {
        Iterator.single((item :: Nil, summary.count)) ++
          project(item).extract(minCount).map { case (t, c) =>
            (item :: t, c)
          }
      } else {
        Iterator.empty
      }
    }
  }
}

object CanTreeV1 {

  /** Representing a node in an FP-Tree. */
  class Node[T](val parent: Node[T]) extends Serializable {
    var item: T = _
    var count: Long = 0L
    val children: mutable.Map[T, Node[T]] = mutable.Map.empty

    def isRoot: Boolean = parent == null
  }

  /** Summary of an item in an FP-Tree. */
  private class Summary[T] extends Serializable {
    var count: Long = 0L
    val nodes: ListBuffer[Node[T]] = ListBuffer.empty
  }
}
