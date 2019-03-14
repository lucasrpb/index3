package index

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

trait Storage {

  def get(id: Array[Byte]): Future[Option[Block]]
  def put(block: Block): Future[Boolean]
  def save(blocks: TrieMap[Array[Byte], Block]): Future[Boolean]

}
