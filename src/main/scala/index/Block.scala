package index

trait Block[T, K, V] {

  val id: T

  def isFull(): Boolean
  def isEmpty(): Boolean
  def hasMinimumSize(): Boolean
  def hasEnoughSize(): Boolean
  def max: Option[K]

}