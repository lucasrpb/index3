package index

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

object Query {

  def inOrder[T](start: Option[Array[Byte]])(implicit ec: ExecutionContext, store: Storage): Seq[Pair] = {
    start match {
      case None => Seq.empty[Pair]
      case Some(id) =>
        Await.result(store.get(id), 1 minute) match {
          case None => Seq.empty[Pair]
          case Some(start) => start match {
            case leaf: Partition => leaf.inOrder()
            case meta: MetaBlock =>
              meta.pointers.slice(0, meta.length).foldLeft(Seq.empty[Pair]) { case (b, (_, n)) =>
                b ++ inOrder(Some(n))
              }
          }
        }
    }
  }

}