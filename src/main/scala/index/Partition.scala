package index

trait Partition extends Block {

  def insert(data: Seq[Pair]): (Boolean, Int)
  def remove(keys: Seq[Array[Byte]]): (Boolean, Int)
  def update(data: Seq[Pair]): (Boolean, Int)
  def inOrder(): Seq[Pair]

  /*def canBorrowTo(t: Partition[T]): Boolean
  def borrowRightTo(t: Partition[T]): Partition[T]
  def borrowLeftTo(t: Partition[T]): Partition[T]
  def merge(r: Partition[T]): Partition[T]
  */
}