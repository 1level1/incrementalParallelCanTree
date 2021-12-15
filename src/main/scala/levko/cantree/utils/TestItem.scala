package levko.cantree.utils

import scala.reflect.ClassTag

class TestItem[Item : ClassTag](var value : Item ) {
  private var groupValue : Int = _

  def this(value : Item , groupValue : Int) {
    this(value)
    this.groupValue = groupValue
  }

  override def hashCode(): Int = {
    this.groupValue
  }
}
