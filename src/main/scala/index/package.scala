import scala.collection.concurrent.TrieMap

package object index {

  type Pair = Tuple2[Array[Byte], Array[Byte]]
  type Pointer[T] = Tuple2[Array[Byte], Block[T, Array[Byte], Array[Byte]]]

  val BLOCK_ADDRESS_SIZE = 8

  type Parents[T] = TrieMap[Block[T, Array[Byte], Array[Byte]], (Option[MetaBlock[T]], Int)]

  case class TxContext[T](){
    val parents: Parents[T] = new Parents[T]()
    val blocks = new TrieMap[Block[T, Array[Byte], Array[Byte]], Boolean]()
  }

  case class IndexRef[T](id: T, root: Option[Block[T, Array[Byte], Array[Byte]]] = None, size: Int = 0)

}
