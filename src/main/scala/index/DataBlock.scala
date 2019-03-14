package index

import java.util.UUID

import scala.reflect.ClassTag

class DataBlock(override val id: Array[Byte],
                val MIN: Int,
                val MAX: Int,
                val LIMIT: Int)(implicit ord: Ordering[Array[Byte]]) extends Partition {

  var length = 0
  var size = 0
  var keys = Array.empty[Pair]

  def find(k: Array[Byte], start: Int, end: Int): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, keys(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return find(k, start, pos - 1)

    find(k, pos + 1, end)
  }

  def insertAt(k: Array[Byte], v: Array[Byte], idx: Int): (Boolean, Int) = {
    for(i<-length until idx by -1){
      keys(i) = keys(i - 1)
    }

    keys(idx) = k -> v
    size += (k.length + v.length)

    length += 1

    true -> idx
  }

  def insert(k: Array[Byte], v: Array[Byte]): (Boolean, Int) = {
    if(size + (k.length + v.length) > MAX) {
      return false -> 0
    }

    val (found, idx) = find(k, 0, length - 1)

    if(found) {
      return false -> 0
    }

    insertAt(k, v, idx)
  }

  def calcMaxLen(data: Seq[Pair], max: Int): Int = {
    var i = 0
    var bytes = 0

    while(i < data.length){
      val (k, v) = data(i)
      bytes += k.length + v.length

      if(bytes >= max) return i

      i += 1
    }

    data.length
  }

  override def insert(data: Seq[Pair]): (Boolean, Int) = {

    val len = calcMaxLen(data, MAX - size)

    for(i<-0 until len){
      val (k, _) = data(i)

      if(find(k, 0, length - 1)._1){
        return false -> 0
      }
    }

    keys = keys ++ Array.ofDim[Pair](len)

    for(i<-0 until len){
      val (k, v) = data(i)
      if(!insert(k, v)._1) {
        return false -> 0
      }
    }

    true -> len
  }

  def removeAt(idx: Int): Pair = {
    val data = keys(idx)

    length -= 1
    size -= (data._1.length + data._2.length)

    for(i<-idx until length){
      keys(i) = keys(i + 1)
    }

    data
  }

  def remove(k: Array[Byte]): Boolean = {
    if(isEmpty()) return false

    val (found, idx) = find(k, 0, length - 1)

    if(!found) return false

    removeAt(idx)

    true
  }

  override def remove(keys: Seq[Array[Byte]]): (Boolean, Int) = {
    if(isEmpty()) return false -> 0

    val len = keys.length

    for(i<-0 until len){
      if(!remove(keys(i))) return false -> 0
    }

    true -> len
  }

  def slice(from: Int, m: Int): Seq[Pair] = {
    var slice = Seq.empty[Pair]

    val len = from + m

    for(i<-from until len){
      slice = slice :+ keys(i)
    }

    for(i<-(from + m) until length){
      keys(i - m) = keys(i)
    }

    length -= m

    slice
  }

  override def update(data: Seq[Pair]): (Boolean, Int) = {

    val len = data.length

    for(i<-0 until len){
      val (k, v) = data(i)

      val (found, idx) = find(k, 0, length - 1)

      if(!found) return false -> 0

      val (_, vOld) = keys(idx)
      keys(idx) = k -> v

      size -= vOld.length
      size += v.length
    }

    true -> len
  }

  override def max: Option[Array[Byte]] = {
    if(isEmpty()) return None
    Some(keys(length - 1)._1)
  }

  override def isFull(): Boolean = size >= LIMIT
  override def isEmpty(): Boolean = size == 0
  override def hasMinimumSize(): Boolean = size >= MIN
  override def hasEnoughSize(): Boolean = size > MIN

  override def inOrder(): Seq[Pair] = {
    if(isEmpty()) return Seq.empty[Pair]
    keys.slice(0, length)
  }

  override def toString(): String = {
    inOrder().map(_._1).mkString("(", "," ,")")
  }

}
