package index

import java.util.UUID
import scala.reflect.ClassTag

class Index[T: ClassTag](var iref: IndexRef[T],
                         val DATA_SIZE: Int,
                         val META_SIZE: Int,
                         val DATA_FILL_FACTOR: Int,
                         val META_FILL_FACTOR: Int)(implicit ord: Ordering[Array[Byte]]){

  val id = UUID.randomUUID.toString.asInstanceOf[T]

  val DATA_MAX = DATA_SIZE
  val DATA_MIN = DATA_MAX/2
  val DATA_LIMIT = (DATA_MAX * 80)/100

  val META_MAX = META_SIZE
  val META_MIN = META_MAX/2
  val META_LIMIT = (META_MAX * 80)/100

  var root = iref.root
  var size = iref.size

  implicit val ctx = new TxContext[T]()

  def ref: IndexRef[T] = IndexRef(id, root, size)

  def fixRoot(p: Block[T, Array[Byte], Array[Byte]]): Boolean = {
    p match {
      case p: MetaBlock[T] =>

        if(p.length == 1){
          root = Some(p.pointers(0)._2)
          true
        } else {
          root = Some(p)
          true
        }

      case p: Partition[T] =>
        root = Some(p)
        true
    }
  }

  def recursiveCopy(p: Block[T, Array[Byte], Array[Byte]]): Boolean = {
    val (parent, pos) = ctx.parents(p)

    parent match {
      case None => fixRoot(p)
      case Some(parent) =>
        val PARENT = parent.copy()
        PARENT.setChild(p.max.get, p, pos)
        recursiveCopy(PARENT)

    }
  }

  def find(k: Array[Byte], start: Option[Block[T, Array[Byte], Array[Byte]]]): Option[Partition[T]] = {
    start match {
      case None => None
      case Some(start) => start match {
        case leaf: DataBlock[T] => Some(leaf)
        case meta: MetaBlock[T] =>

          val length = meta.length
          val pointers = meta.pointers

          for(i<-0 until length){
            val child = pointers(i)._2
            ctx.parents += child -> (Some(meta), i)
          }

          find(k, meta.findPath(k))
      }
    }
  }

  def find(k: Array[Byte]): Option[Partition[T]] = {
    if(root.isDefined){
      ctx.parents += root.get -> (None, 0)
    }

    find(k, root)
  }

  def insertEmptyIndex(data: Seq[Pair]): (Boolean, Int) = {
    val p = new DataBlock[T](UUID.randomUUID.toString.asInstanceOf[T], DATA_MIN, DATA_MAX, DATA_LIMIT)

    val (ok, n) = p.insert(data)
    ctx.parents += p -> (None, 0)

    (ok && recursiveCopy(p)) -> n
  }

  def insertParent(left: MetaBlock[T], prev: Block[T, Array[Byte], Array[Byte]]): Boolean = {
    if(left.isFull()){
      val right = left.split()

      if(ord.gt(prev.max.get, left.max.get)){
        right.insert(Seq(prev.max.get -> prev))
      } else {
        left.insert(Seq(prev.max.get -> prev))
      }

      return handleParent(left, right)
    }

    left.insert(Seq(prev.max.get  -> prev))

    recursiveCopy(left)
  }

  def handleParent(left: Block[T, Array[Byte], Array[Byte]], right: Block[T, Array[Byte], Array[Byte]]): Boolean = {
    val (parent, pos) = ctx.parents(left)

    parent match {
      case None =>

        val meta = new MetaBlock[T](UUID.randomUUID.toString.asInstanceOf[T], META_MIN, META_MAX, META_LIMIT)

        ctx.parents += meta -> (None, 0)

        meta.insert(Seq(
          left.max.get -> left,
          right.max.get -> right
        ))

        recursiveCopy(meta)

      case Some(parent) =>

        val PARENT = parent.copy()
        PARENT.setChild(left.max.get, left, pos)

        insertParent(PARENT, right)
    }
  }

  def insertLeaf(leaf: Partition[T], data: Seq[Pair]): (Boolean, Int) = {
    val left = copyPartition(leaf)

    if(leaf.isFull()){

      val x = left.asInstanceOf[DataBlock[T]]

      println(s"left ${x.length} ${x.size}\n")

      val right = left.split()
      return handleParent(left, right) -> 0
    }

    val (ok, n) = left.insert(data)

    (ok && recursiveCopy(left)) -> n
  }

  def insert(data: Seq[Pair]): (Boolean, Int) = {

    val sorted = data.sortBy(_._1)
    val size = sorted.length
    var pos = 0

    while(pos < size){

      var list = sorted.slice(pos, size)
      val (k, _) = list(0)

      val (ok, n) = find(k) match {
        case None => insertEmptyIndex(list)
        case Some(leaf) =>

          val idx = list.indexWhere {case (k, _) => ord.gt(k, leaf.max.get)}
          if(idx > 0) list = list.slice(0, idx)

          insertLeaf(leaf, list)
      }

      if(!ok) return false -> 0

      pos += n
    }

    this.size += size

    //println(s"inserting ${data.map(_._1)}...\n")

    true -> size
  }

  def copyPartition(p: Partition[T]): Partition[T] = {
    if(ctx.blocks.isDefinedAt(p)) return p

    val copy = p.copy()

    ctx.blocks += copy -> true
    ctx.parents += copy -> ctx.parents(p)

    copy
  }

}