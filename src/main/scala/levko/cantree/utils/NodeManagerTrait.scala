package levko.cantree.utils

import scala.collection.mutable

trait NodeManagerTrait[T] {
  import CanTree.Node
  def addNode(node: Node[T])
//  def getFrequentNodes[T](minFreq : Int) : Option[List[Node[T]]];
  def getNodeRankMap():mutable.HashMap[T,Long]

  def getSize():Long
}
