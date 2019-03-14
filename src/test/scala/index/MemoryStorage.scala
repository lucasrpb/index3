package index

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.reflect.ClassTag

class MemoryStorage() extends Storage {

  val blocks = new TrieMap[Array[Byte], Block]()

  override def get(id: Array[Byte]): Future[Option[Block]] = {
    Future.successful(blocks.get(id))
  }

  override def save(blocks: TrieMap[Array[Byte], Block]): Future[Boolean] = {
    blocks.foreach{case (id, b) => this.blocks.put(id, b)}
    Future.successful(true)
  }

  override def put(block: Block): Future[Boolean] = {
    blocks.put(block.id, block)
    Future.successful(true)
  }

}
