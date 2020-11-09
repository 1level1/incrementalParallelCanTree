package levko.cantree.utils

import org.apache.spark.rdd.RDD

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer

class CanTree[T] extends Serializable {

  import CanTree._

  val root: Node[T] = new Node(null)
  private val leafNodes: mutable.HashMap[Int, Node[T]] = mutable.HashMap.empty
  private val nodeMgr: NodeManagerTrait[T] = new NodeManager[T]()
  //  private val summaries: mutable.Map[T, Summary[T]] = mutable.Map.empty

  /** Adds a transaction with count. */
  def add(t: Iterable[T], count: Long = 1L): CanTree[T] = {
    require(count > 0)
    var curr = root
    var path: ListBuffer[T] = ListBuffer.empty
    //    curr.count += count
    t.foreach { item =>
      val child = curr.children.getOrElseUpdate(item, {
        val newNode = new Node(curr)
        newNode.item = item
        //        summary.nodes += newNode
        nodeMgr.addNode(newNode);
        newNode
      })
      if (curr.isLeaf) {
        curr.isLeaf = false
        leafNodes.remove(path.hashCode())
      }
      child.count += count
      path += item
      if (child.isLeaf)
        leafNodes(path.hashCode()) = child
      curr = child
    }
    this
  }

  /** Merges another FP-Tree. */
  def merge(other: CanTree[T]): CanTree[T] = {
    other.transactions.foreach { case (t, c) =>
      add(t, c)
    }
    this
  }

  private def getRelevantTransactions(node: Node[T], fisMap: immutable.Map[T, Long]): (ListBuffer[T], Long) = {
    var retList: ListBuffer[T] = ListBuffer.empty
    var count: Long = 0L
    var curr: Node[T] = node
    while (curr != null) {
      if (fisMap.contains(curr.item)) {
        retList += curr.item
      }
      curr = curr.parent
    }
    if (retList.nonEmpty)
      count = node.count
    (retList, count)
  }

  def calcTransactions(fisMap: immutable.Map[T, Long]): ListBuffer[(ListBuffer[T], Long)] = {
    var res: ListBuffer[(ListBuffer[T], Long)] = ListBuffer.empty
    for (lNode <- leafNodes.values) {
      var currRes = getRelevantTransactions(lNode, fisMap)
      if (currRes._1.nonEmpty)
        res += currRes
    }
    res
  }


  /** Returns all transactions in an iterator. */
  def transactions: Iterator[(List[T], Long)] = getTransactions(root)

  /** Returns all transactions under this node. */
  private def getTransactions(node: Node[T]): Iterator[(List[T], Long)] = {
    var count = node.count
    node.children.iterator.flatMap { case (item, child) =>
      getTransactions(child).map { case (t, c) =>
        count -= c
        (item :: t, c)
      }
    } ++ {
      if (count > 0) {
        Iterator.single((Nil, count))
      } else {
        Iterator.empty
      }
    }
  }

  private def iterateBottomUp(node: Node[T]): Iterator[T] = {
    if (node == null)
      Iterator.empty
    else
      Iterator.single(node.item) ++ iterateBottomUp(node.parent)
  }

  /** Extracts all patterns with valid suffix and minimum count. */
  def extract(globalFIS: mutable.Map[T, Long]
             ): Iterable[(Iterator[T], Long)] = {
    val ret: mutable.ListBuffer[(Iterator[T], Long)] = mutable.ListBuffer.empty
    for ((k, v) <- leafNodes) {
      if (globalFIS.getOrElse(v.item, null) != null) {
        val currList = iterateBottomUp(v)
        ret.append((currList, v.count))
      }
    }
    ret
  }

  def getNodeRankMap(): mutable.HashMap[T, Long] = {
    return nodeMgr.getNodeRankMap()
  }

  override def toString(): String = {
    val res: ListBuffer[(ListBuffer[T], Long)] = ListBuffer.empty
    for (lNode <- leafNodes.values) {
      val currCount = lNode.count
      var curr = lNode
      val currRes: ListBuffer[T] = ListBuffer.empty
      while (curr != null) {
        currRes.append(curr.item)
        curr = curr.parent
      }
      res.append((currRes, lNode.count))
    }
    nodeMgr.getSize()+","+res.toString()
  }
}

object CanTree {

  /** Representing a node in an FP-Tree. */
  class Node[T](val parent: Node[T],val r: Long = 0) extends Serializable {
    var item: T = _
    var count: Long = 0L
    val children: mutable.Map[T, Node[T]] = mutable.Map.empty
    val parentNode: Node[T] = parent
    val leftLeafNode: Node[T] = null
    val rightLeafNode: Node[T] = null
    var isLeaf: Boolean = true
    val rank : Long = r

  }
}
