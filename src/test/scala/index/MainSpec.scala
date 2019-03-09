package index

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference

import com.google.common.primitives.UnsignedBytes
import org.scalatest.FlatSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class MainSpec extends FlatSpec {

  implicit val ord = new Ordering[Array[Byte]] {
    val comparator = UnsignedBytes.lexicographicalComparator()
    override def compare(x: Array[Byte], y: Array[Byte]): Int = comparator.compare(x, y)
  }

  val MAX_VALUE = Int.MaxValue

  def test(): Unit = {

    val rand = ThreadLocalRandom.current()

    val SIZE = 1024
    val FACTOR = 80

    val ref = new AtomicReference(new IndexRef[String](UUID.randomUUID.toString))
    val old = ref.get()
    val index = new Index[String](old, SIZE, SIZE, FACTOR, FACTOR)

    val words = scala.util.Random.shuffle(Words.words)
    val len = 300//Words.words.length
    val n = len

    var list = Seq.empty[Pair]

    for(i<-0 until n){
      //val k = Words.words(i).getBytes()
      val k = rand.nextInt(1, MAX_VALUE).toString.getBytes()

      if(!list.exists(_._1.equals(k))){
        list = list :+ k -> k
      }
    }

    if(index.insert(list)._1){
      ref.compareAndSet(old, index.ref)
    }

    val dsorted = list.sortBy(_._1).map{case (k, v) => new String(k)}
    val isorted = Query.inOrder(index.ref.root).map{case (k, _) => new String(k)}

    println(s"dsorted: $dsorted\n")
    println(s"isorted: $isorted\n")

    assert(dsorted.equals(isorted))
  }

  "index data " should "be equal to test data" in {

    val n = 1000

    for(i<-0 until n){
      test()
    }

  }

}
