import scala.collection.concurrent.TrieMap

package object index {

  type Pair = Tuple2[Array[Byte], Array[Byte]]
  type Pointer = Tuple2[Array[Byte], String]

  //val BLOCK_ADDRESS_SIZE = 8

  type Parents = TrieMap[String, (Option[String], Int)]

  case class IndexRef(id: String, root: Option[String] = None, size: Int = 0)

}
