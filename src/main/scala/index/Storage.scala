package index

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

trait Storage {

  def get(id: String): Future[Option[Block[String, Array[Byte], Array[Byte]]]]
  def put(block: Block[String, Array[Byte], Array[Byte]]): Future[Boolean]
  def save(blocks: TrieMap[String, Block[String, Array[Byte], Array[Byte]]]): Future[Boolean]

}
