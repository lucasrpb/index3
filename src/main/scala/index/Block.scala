package index

trait Block {

  val id: Array[Byte]

  def isFull(): Boolean
  def isEmpty(): Boolean
  def hasMinimumSize(): Boolean
  def hasEnoughSize(): Boolean
  def max: Option[Array[Byte]]

}