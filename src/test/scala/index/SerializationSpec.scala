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

  val MAX_VALUE = Int.MaxValue

  def test(): Unit = {

    object DataBlockSerializer extends DataBlockSerializer(2048)
    object MetaBlockSerializer extends MetaBlockSerializer(2048)

    val rand = ThreadLocalRandom.current()

    import DataBlockSerializer.{MIN => DATA_MIN, MAX => DATA_MAX, LIMIT => DATA_LIMIT}
    import MetaBlockSerializer.{MIN => META_MIN, MAX => META_MAX, LIMIT => META_LIMIT}

    val b1 = new DataBlock(UUID.randomUUID.toString.getBytes(), DATA_MIN, DATA_MAX, DATA_LIMIT)

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

    val buf = DataBlockSerializer.serialize(b1)

    val b2 = DataBlockSerializer.deserialize(buf).get

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
