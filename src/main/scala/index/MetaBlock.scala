package index

import java.util.UUID

import scala.reflect.ClassTag

class MetaBlock[T: ClassTag](val id: T,
                             val MIN: Int,
                             val MAX: Int,
                             val LIMIT: Int)(implicit ord: Ordering[Array[Byte]])
  extends Block[T, Array[Byte], Array[Byte]]{

  var length = 0
  var size = 0
  var pointers = Array.empty[Pointer[T]]

  def find(k: Array[Byte], start: Int, end: Int): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, pointers(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return find(k, start, pos - 1)

    find(k, pos + 1, end)
  }

  def findPath(k: Array[Byte]): Option[Block[T, Array[Byte], Array[Byte]]] = {
    if(isEmpty()) return None
    val (_, pos) = find(k, 0, length - 1)

    Some(pointers(if(pos < length) pos else pos - 1)._2)
  }

  def setChild(k: Array[Byte], child: Block[T, Array[Byte], Array[Byte]], pos: Int)
              (implicit ctx: TxContext[T]): Boolean = {
    pointers(pos) = k -> child

    ctx.parents += child -> (Some(this), pos)

    true
  }

  def left[S <: Block[T, Array[Byte], Array[Byte]]](pos: Int): Option[S] = {
    val lpos = pos - 1
    if(lpos < 0) return None
    Some(pointers(lpos)._2.asInstanceOf[S])
  }

  def right[S <: Block[T, Array[Byte], Array[Byte]]](pos: Int): Option[S] = {
    val rpos = pos + 1
    if(rpos >= length) return None
    Some(pointers(rpos)._2.asInstanceOf[S])
  }

  def insertAt(k: Array[Byte], child: Block[T, Array[Byte], Array[Byte]], idx: Int)
              (implicit ctx: TxContext[T]): (Boolean, Int) = {
    for(i<-length until idx by -1){
      val (k, child) = pointers(i - 1)
      setChild(k, child, i)
    }

    setChild(k, child, idx)

    length += 1
    size += (k.length + BLOCK_ADDRESS_SIZE)

    true -> idx
  }

  def insert(k: Array[Byte], child: Block[T, Array[Byte], Array[Byte]])
            (implicit ctx: TxContext[T]): (Boolean, Int) = {
    if(size + (k.length + BLOCK_ADDRESS_SIZE) > MAX) {
      return false -> 0
    }

    val (found, idx) = find(k, 0, length - 1)

    if(found) return false -> 0

    insertAt(k, child, idx)
  }

  def calcMaxLen(data: Seq[Pointer[T]], max: Int): Int = {
    var i = 0
    var bytes = 0

    while(i < data.length){
      val (k, c) = data(i)
      bytes += k.length + BLOCK_ADDRESS_SIZE

      if(bytes >= max) return i

      i += 1
    }

    data.length
  }

  def insert(data: Seq[Pointer[T]])(implicit ctx: TxContext[T]): (Boolean, Int) = {

    val len = calcMaxLen(data, MAX - size)

    for(i<-0 until len){
      val (k, _) = data(i)

      if(find(k, 0, length - 1)._1){
        return false -> 0
      }
    }

    pointers = pointers ++ Array.ofDim[Pointer[T]](len)

    for(i<-0 until len){
      val (k, c) = data(i)
      if(!insert(k, c)._1) {
        return false -> 0
      }
    }

    true -> len
  }

  def removeAt(idx: Int)(implicit ctx: TxContext[T]): Pointer[T] = {
    val data = pointers(idx)

    length -= 1
    size -= (data._1.length + BLOCK_ADDRESS_SIZE)

    for(i<-idx until length){
      val (k, child) = pointers(i + 1)
      setChild(k, child, i)
    }

    data
  }

  def remove(k: Array[Byte])(implicit ctx: TxContext[T]): Boolean = {
    if(isEmpty()) return false

    val (found, idx) = find(k, 0, length - 1)

    if(!found) return false

    removeAt(idx)

    true
  }

  def remove(keys: Seq[Array[Byte]])(implicit ctx: TxContext[T]): (Boolean, Int) = {
    if(isEmpty()) return false -> 0

    val len = keys.length

    for(i<-0 until len){
      if(!remove(keys(i))) return false -> 0
    }

    true -> len
  }

  def update(data: Seq[Pointer[T]])(implicit ctx: TxContext[T]): (Boolean, Int) = {

    val len = data.length

    for(i<-0 until len){
      val (k, child) = data(i)

      val (found, idx) = find(k, 0, length - 1)

      if(!found) return false -> 0

      setChild(k, child, idx)
    }

    true -> len
  }

  def split()(implicit ctx: TxContext[T]): MetaBlock[T] = {
    val right = new MetaBlock[T](UUID.randomUUID.toString.asInstanceOf[T], MIN, MAX, LIMIT)

    val len = length
    val middle = len/2

    right.pointers = Array.ofDim[Pointer[T]](len - middle)

    for(i<-middle until len){
      val (k, child) = pointers(i)
      right.setChild(k, child, i - middle)

      right.size += k.length + BLOCK_ADDRESS_SIZE
      size -= (k.length + BLOCK_ADDRESS_SIZE)

      right.length += 1
      length -= 1
    }

    right
  }

  def copy()(implicit ctx: TxContext[T]): MetaBlock[T] = {

    if(ctx.blocks.isDefinedAt(this)) return this

    val copy = new MetaBlock[T](UUID.randomUUID.toString.asInstanceOf[T], MIN, MAX, LIMIT)

    copy.size = size
    copy.length = length
    copy.pointers = Array.ofDim[Pointer[T]](length)

    ctx.blocks += copy -> true
    ctx.parents += copy -> ctx.parents(this)

    for(i<-0 until length){
      val (k, child) = pointers(i)
      copy.setChild(k, child, i)
    }

    return this
  }

  def max: Option[Array[Byte]] = {
    if(isEmpty()) return None
    Some(pointers(length - 1)._1)
  }

  def isFull(): Boolean = size >= LIMIT
  def isEmpty(): Boolean = size == 0
  def hasMinimumSize(): Boolean = size >= MIN
  def hasEnoughSize(): Boolean = size > MIN

  def inOrder(): Seq[Pointer[T]] = {
    if(isEmpty()) return Seq.empty[Pointer[T]]
    pointers.slice(0, length)
  }

  /*override def toString(): String = {
    inOrder().map(_._1).mkString("(", "," ,")")
  }*/

}
