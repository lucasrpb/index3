package index

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class MemoryContext(val DATA_SIZE: Int,
                    val META_SIZE: Int,
                    val DATA_FILL_FACTOR: Int,
                    val META_FILL_FACTOR: Int)(implicit val ec: ExecutionContext,
                                               val store: Storage,
                                               val ord: Ordering[Array[Byte]]) extends TxContext {

  val MIN_DATA_FACTOR = 100 - DATA_FILL_FACTOR
  val MIN_META_FACTOR = 100 - META_FILL_FACTOR

  val DATA_MAX = DATA_SIZE
  val DATA_MIN = (DATA_MAX * MIN_DATA_FACTOR)/100
  val DATA_LIMIT = (DATA_MAX * 80)/100

  val META_MAX = META_SIZE
  val META_MIN = (META_MAX * MIN_META_FACTOR)/100
  val META_LIMIT = (META_MAX * 80)/100

  println(s"DATA_MIN ${DATA_MIN}, DATA_MAX ${DATA_MAX} DATA_LIMIT ${DATA_LIMIT}\n")

  override def createPartition(): Partition = {
    val p = new DataBlock(UUID.randomUUID.toString, DATA_MIN, DATA_MAX, DATA_LIMIT)
    blocks.put(p.id, p)
    p
  }

  override def createMeta(): MetaBlock = {
    val m = new MetaBlock(UUID.randomUUID.toString, META_MIN, META_MAX, META_LIMIT)
    blocks.put(m.id, m)
    m
  }

  override def getBlock(id: String): Future[Option[Block[String, Array[Byte], Array[Byte]]]] = {
    blocks.get(id) match {
      case None => store.get(id)
      case Some(b) => Future.successful(Some(b))
    }
  }

  override def getPartition(id: String): Future[Option[Partition]] = {
    getBlock(id).map(_.map(_.asInstanceOf[Partition]))
  }

  override def getMeta(id: String): Future[Option[MetaBlock]] = {
    getBlock(id).map(_.map(_.asInstanceOf[MetaBlock]))
  }

  override def copy(p: Partition): Partition = {
    if(blocks.isDefinedAt(p.id)) return p

    val src = p.asInstanceOf[DataBlock]
    val copy = createPartition().asInstanceOf[DataBlock]

    copy.length = src.length
    copy.size = src.size

    copy.keys = Array.ofDim[Pair](src.length)

    for(i<-0 until src.length){
      copy.keys(i) = src.keys(i)
    }

    parents += copy.id -> parents(p.id)

    copy
  }

  override def copy(src: MetaBlock): MetaBlock = {
    if(blocks.isDefinedAt(src.id)) return src

    val copy = createMeta()

    copy.size = src.size
    copy.length = src.length
    copy.pointers = Array.ofDim[Pointer](src.length)

    parents += copy.id -> parents(src.id)

    for(i<-0 until src.length){
      val (k, child) = src.pointers(i)
      copy.setChild(k, child, i)(this)
    }

    copy
  }

  override def split(p: Partition): Partition = {
    val src = p.asInstanceOf[DataBlock]
    val right = createPartition().asInstanceOf[DataBlock]

    val len = src.calcMaxLen(src.keys, src.size/2)//src.length
    val middle = len/2

    right.keys = Array.ofDim[Pair](len - middle)

    for(i<-middle until len){

      val (k, v) = src.keys(i)

      right.keys(i - middle) = src.keys(i)

      right.size += (k.length + v.length)
      src.size -= (k.length + v.length)

      right.length += 1
      src.length -= 1
    }

    right
  }

  override def split(src: MetaBlock): MetaBlock = {
    val right = createMeta()

    val len = src.calcMaxLen(src.pointers, src.size/2)//src.length
    val middle = len/2

    right.pointers = Array.ofDim[Pointer](len - middle)

    for(i<-middle until len){
      val (k, child) = src.pointers(i)
      right.setChild(k, child, i - middle)(this)

      right.size += k.length + child.length
      src.size -= (k.length + child.length)

      right.length += 1
      src.length -= 1
    }

    right
  }

}
