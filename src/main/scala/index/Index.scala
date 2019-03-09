package index

import java.util.UUID

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

class Index[T: ClassTag](var iref: IndexRef)
                        (implicit val ord: Ordering[Array[Byte]], val ctx: TxContext, val ec: ExecutionContext){

  val id = UUID.randomUUID.toString

  var root = iref.root
  var size = iref.size

  //implicit val ctx = new TxContext[T]()

  def ref: IndexRef = IndexRef(id, root, size)

  def fixRoot(p: Block[String, Array[Byte], Array[Byte]]): Boolean = {
    p match {
      case p: MetaBlock =>

        if(p.size == 1){
          root = Some(p.pointers(0)._2)
          true
        } else {
          root = Some(p.id)
          true
        }

      case p: Partition =>
        root = Some(p.id)
        true
    }
  }

  def recursiveCopy(p: Block[String, Array[Byte], Array[Byte]]): Future[Boolean] = {
    val (parent, pos) = ctx.parents(p.id)

    parent match {
      case None => Future.successful(fixRoot(p))
      case Some(id) => ctx.getMeta(id).flatMap { opt =>
        opt match {
          case None => Future.successful(false)
          case Some(parent) =>

            val PARENT = ctx.copy(parent)
            PARENT.setChild(p.max.get, p.id, pos)
            recursiveCopy(PARENT)
        }
      }
    }
  }

  def find(k: Array[Byte], start: Option[String]): Future[(Boolean, Option[Partition])] = {
    start match {
      case None => Future.successful(false -> None)
      case Some(id) => ctx.getBlock(id).flatMap { opt =>
        opt match {
          case None => Future.successful(false -> None)
          case Some(start) => start match {
            case leaf: Partition => Future.successful(true -> Some(leaf))
            case meta: MetaBlock =>

              val length = meta.length
              val pointers = meta.pointers

              for(i<-0 until length){
                val child = pointers(i)._2
                ctx.parents += child -> (Some(meta.id), i)
              }

              find(k, meta.findPath(k))
          }
        }
      }
    }
  }

  def find(k: Array[Byte]): Future[(Boolean, Option[Partition])] = {
    root match {
      case None => Future.successful(true -> None)
      case Some(id) =>

        ctx.parents += id -> (None, 0)

        find(k,root)
    }
  }

  def insertEmptyIndex(data: Seq[Pair]): Future[(Boolean, Int)] = {
    val p = ctx.createPartition()

    val (ok, n) = p.insert(data)
    ctx.parents += p.id -> (None, 0)

    if(!ok) return Future.successful(false -> 0)

    recursiveCopy(p).map(_ -> n)
  }

  def insertParent(left: MetaBlock, prev: Block[String, Array[Byte], Array[Byte]]): Future[Boolean] = {
    if(left.isFull()){
      val right = ctx.split(left)

      if(ord.gt(prev.max.get, left.max.get)){
        right.insert(Seq(prev.max.get -> prev.id))
      } else {
        left.insert(Seq(prev.max.get -> prev.id))
      }

      return handleParent(left, right)
    }

    left.insert(Seq(prev.max.get -> prev.id))

    recursiveCopy(left)
  }

  def handleParent(left: Block[String, Array[Byte], Array[Byte]],
                   right: Block[String, Array[Byte], Array[Byte]]): Future[Boolean] = {
    val (parent, pos) = ctx.parents(left.id)

    parent match {
      case None =>

        val meta = ctx.createMeta()

        ctx.parents += meta.id -> (None, 0)

        meta.insert(Seq(
          left.max.get -> left.id,
          right.max.get -> right.id
        ))

        recursiveCopy(meta)

      case Some(id) => ctx.getMeta(id).flatMap { opt =>
        opt match {
          case None => Future.successful(false)
          case Some(parent) =>

            val PARENT = ctx.copy(parent)
            PARENT.setChild(left.max.get, left.id, pos)

            insertParent(PARENT, right)
        }
      }
    }
  }

  def insertLeaf(leaf: Partition, data: Seq[Pair]): Future[(Boolean, Int)] = {
    val left = ctx.copy(leaf)

    if(left.isFull()){
      val right = ctx.split(left)
      return handleParent(left, right).map(_ -> 0)
    }

    val (ok, n) = left.insert(data)

    if(!ok) return Future.successful(false -> 0)

    recursiveCopy(left).map(_ -> n)
  }

  def insert(data: Seq[Pair]): Future[(Boolean, Int)] = {

    val sorted = data.sortBy(_._1)
    val size = sorted.length
    var pos = 0

    def insert(): Future[(Boolean, Int)] = {
      if(pos == size) return Future.successful(true -> 0)

      var list = sorted.slice(pos, size)
      val (k, _) = list(0)

      find(k).flatMap {
        _ match {
          case (true, None) => insertEmptyIndex(list)
          case (true, Some(leaf)) =>

            val idx = list.indexWhere {case (k, _) => ord.gt(k, leaf.max.get)}
            if(idx > 0) list = list.slice(0, idx)

            insertLeaf(leaf, list)

          case _ => Future.successful(false -> 0)
        }
      }.flatMap { case (ok, n) =>
        if(!ok) {
          Future.successful(false -> 0)
        } else {
          pos += n
          insert()
        }
      }
    }

    insert().map { case (ok, _) =>
      if(ok){
        this.size += size
      }

      ok -> size
    }
  }

}