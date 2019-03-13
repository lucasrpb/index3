package index

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class MemoryContext(val DATA_SIZE: Int,
                    val META_SIZE: Int)(implicit val ec: ExecutionContext,
                                               val store: Storage,
                                               val ord: Ordering[Array[Byte]]) extends TxContext {

  val DATA_MAX = DATA_SIZE
  val DATA_MIN = DATA_MAX/3
  val DATA_LIMIT = 2 * DATA_MIN

  val META_MAX = META_SIZE
  val META_MIN = META_MAX/3
  val META_LIMIT = 2 * META_MIN

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

    val len = src.length
    val middle = if(src.length < 3) 1 else
      src.calcMaxLen(src.keys, src.size/2)

    right.keys = Array.ofDim[Pair](len - middle)

    for(i<-middle until len){

      val data = src.keys(i)

      right.keys(i - middle) = data

      right.size += (data._1.length + data._2.length)
      src.size -= (data._1.length + data._2.length)

      right.length += 1
      src.length -= 1
    }

    right
  }

  override def split(src: MetaBlock): MetaBlock = {
    val right = createMeta()

    val len = src.length
    val middle = if(src.length < 3) 1 else
      src.calcMaxLen(src.pointers, src.size/2)

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
