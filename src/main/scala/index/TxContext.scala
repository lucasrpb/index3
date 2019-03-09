package index

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

trait TxContext {

  val parents: Parents = new Parents()
  val blocks = new TrieMap[String, Block[String, Array[Byte], Array[Byte]]]()

  def createPartition(): Partition
  def createMeta(): MetaBlock

  def getBlock(id: String): Future[Option[Block[String, Array[Byte], Array[Byte]]]]
  def getPartition(id: String): Future[Option[Partition]]
  def getMeta(id: String): Future[Option[MetaBlock]]

  def copy(meta: MetaBlock): MetaBlock
  def copy(p: Partition): Partition

  def split(meta: MetaBlock): MetaBlock
  def split(p: Partition): Partition

}
