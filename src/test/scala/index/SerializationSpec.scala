package index

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference

import com.google.common.primitives.UnsignedBytes
import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class SerializationSpec extends FlatSpec {

  implicit val ord = new Ordering[Array[Byte]] {
    val comparator = UnsignedBytes.lexicographicalComparator()
    override def compare(x: Array[Byte], y: Array[Byte]): Int = comparator.compare(x, y)
  }

  object BlockSerializer extends Serializer [DataBlock]{

    val SIZE = 2048

    val DATA_MAX = SIZE
    val DATA_MIN = DATA_MAX/3
    val DATA_LIMIT = 2 * DATA_MIN

    val HEADER_SIZE = 8

    override def serialize(b: DataBlock): ByteBuffer = {
      val meta_size = b.length * 8
      val buf = ByteBuffer.allocateDirect(HEADER_SIZE + meta_size + b.size)

      // Header
      buf.putInt(b.length)

      // keys size
      buf.putInt(b.size)

      for(i<-0 until b.length){
        val (k, v) = b.keys(i)
        val klen = k.length
        val vlen = v.length

        //key size
        buf.putInt(k.length).putInt(vlen)

        //Content
        buf.put(k)
        buf.put(v)
      }

      buf.flip()
    }

    override def deserialize(buf: ByteBuffer): Option[DataBlock] = {
      val b = new DataBlock(UUID.randomUUID.toString, DATA_MIN, DATA_MAX, DATA_LIMIT)

      b.length = buf.getInt()
      b.size = buf.getInt()

      b.keys = Array.ofDim[Pair](b.length)

      for(i<-0 until b.length){
        val klen = buf.getInt()
        val vlen = buf.getInt()

        val k = Array.ofDim[Byte](klen)

        buf.get(k)

        val v = Array.ofDim[Byte](vlen)

        buf.get(v)

        b.keys(i) = k -> v
      }

      Some(b)
    }
  }

  val MAX_VALUE = Int.MaxValue

  def test(): Unit = {

    val rand = ThreadLocalRandom.current()

    import BlockSerializer._

    val b1 = new DataBlock(UUID.randomUUID.toString, DATA_MIN, DATA_MAX, DATA_LIMIT)

    var list = Seq.empty[Pair]

    val n = 100

    for(i<-0 until n){
      val k = rand.nextInt(1, MAX_VALUE).toString.getBytes()

      if(!list.exists{case (k1, _) => ord.equiv(k, k1)}){
        list = list :+ k -> k
      }
    }

    println(b1.insert(list))
    println(b1.length, b1.size)

    val buf = BlockSerializer.serialize(b1)

    val b2 = BlockSerializer.deserialize(buf).get

    val b1s = b1.inOrder().map{case (k, v) => new String(k)}
    val b2s = b2.inOrder().map{case (k, v) => new String(k)}

    println(s"b1s: ${b1s}\n")
    println(s"b2s: ${b2s}\n")

    assert(b1s.equals(b2s))
  }

  "index data " should "be equal to test data" in {

    val n = 1

    for(i<-0 until n){
      test()
    }

  }

}
