package index
import java.nio.ByteBuffer
import java.util.UUID

class DataBlockSerializer(val SIZE: Int)(implicit val ord: Ordering[Array[Byte]]) extends Serializer[DataBlock]{

  val MAX = SIZE
  val MIN = MAX/3
  val LIMIT = 2 * MIN

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
    val b = new DataBlock(UUID.randomUUID.toString.getBytes(), MIN, MAX, LIMIT)

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
