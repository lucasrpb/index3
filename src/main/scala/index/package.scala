import scala.collection.concurrent.TrieMap

package object index {

  type Pair = Tuple2[Array[Byte], Array[Byte]]
  type Pointer = Tuple2[Array[Byte], Array[Byte]]

  //val BLOCK_ADDRESS_SIZE = 8

  type Parents = TrieMap[Array[Byte], (Option[Array[Byte]], Int)]

  case class IndexRef(id: String, root: Option[Array[Byte]] = None, size: Int = 0)

}
