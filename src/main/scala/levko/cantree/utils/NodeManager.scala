package levko.cantree.utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class NodeManager[T] extends Serializable with NodeManagerTrait[T]  {

  import CanTree.Node;

  private val nodeMap : mutable.HashMap[T,ListBuffer[Node[T]]] = mutable.HashMap.empty;
  private var nodesSize : Long = 0L

  override def getSize(): Long = {nodesSize}
  def addNode(node : Node[T]) : Unit = {
    require(node!=null);
    val key = node.item;
    require(key!=null)
    val mapEntry = nodeMap.getOrElseUpdate(key, {
      nodesSize+=1
      mutable.ListBuffer.empty
    })
    mapEntry.append(node);
  }

  def getNodeRankMap() : mutable.HashMap[T,Long] = {
    val res = mutable.HashMap.empty[T,Long]
    for ((k,nodeList)<-nodeMap) {
      var currCnt: Long = 0L;
      for (n <-nodeList) {
        currCnt+=n.count
      }
      res(k)=currCnt
    }
    return res
  }

}
