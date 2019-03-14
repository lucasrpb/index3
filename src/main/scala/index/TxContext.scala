package index

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

trait TxContext {

  val parents: Parents = new Parents()
  val blocks = new TrieMap[Array[Byte], Block]()

  def createPartition(): Partition
  def createMeta(): MetaBlock

  def getBlock(id: Array[Byte]): Future[Option[Block]]
  def getPartition(id: Array[Byte]): Future[Option[Partition]]
  def getMeta(id: Array[Byte]): Future[Option[MetaBlock]]

  def copy(meta: MetaBlock): MetaBlock
  def copy(p: Partition): Partition

  def split(meta: MetaBlock): MetaBlock
  def split(p: Partition): Partition

}
