package index

object Query {

  def inOrder[T](start: Option[Block[T, Array[Byte], Array[Byte]]]): Seq[Pair] = {
    start match {
      case None => Seq.empty[Pair]
      case Some(start) => start match {
        case leaf: Partition[T] =>

          val meta = leaf.asInstanceOf[DataBlock[T]]

          println(s"data size: ${meta.size} length: ${meta.length} isFull ${meta.isFull()} min ${meta.hasMinimumSize()} " +
            s"enough ${meta.hasEnoughSize()}\n")

          leaf.inOrder()
        case meta: MetaBlock[T] =>

          println(s"meta size: ${meta.size} length: ${meta.length} isFull ${meta.isFull()} min ${meta.hasMinimumSize()} " +
            s"enough ${meta.hasEnoughSize()}\n")

          meta.pointers.slice(0, meta.length).foldLeft(Seq.empty[Pair]) { case (b, (_, n)) =>
            b ++ inOrder(Some(n))
          }
      }
    }
  }

}