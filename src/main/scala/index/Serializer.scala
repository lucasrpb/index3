package index

import java.nio.ByteBuffer

trait Serializer[T] {

  def serialize(o: T): ByteBuffer
  def deserialize(buf: ByteBuffer): Option[T]

}
