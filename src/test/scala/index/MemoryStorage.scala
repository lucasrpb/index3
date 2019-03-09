package index

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.reflect.ClassTag

class MemoryStorage() extends Storage {

  val blocks = new TrieMap[String, Block[String, Array[Byte], Array[Byte]]]()

  override def get(id: String): Future[Option[Block[String, Array[Byte], Array[Byte]]]] = {
    Future.successful(blocks.get(id))
  }

  override def save(blocks: TrieMap[String, Block[String, Array[Byte], Array[Byte]]]): Future[Boolean] = {
    blocks.foreach{case (id, b) => this.blocks.put(id, b)}
    Future.successful(true)
  }

  override def put(block: Block[String, Array[Byte], Array[Byte]]): Future[Boolean] = {
    blocks.put(block.id, block)
    Future.successful(true)
  }

}
