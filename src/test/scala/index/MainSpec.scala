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

    val SIZE = 1024//rand.nextInt(100, 2048)
    val FACTOR = 80

    implicit val store = new MemoryStorage()
    val root = new AtomicReference[IndexRef](IndexRef(UUID.randomUUID.toString))

    def insert(): Future[(Int, Boolean, Seq[Pair])] = {

      implicit val ctx = new MemoryContext(SIZE, SIZE, FACTOR, FACTOR)
      val old = root.get()
      val index = new Index[String](old, ctx.DATA_MIN)

      val n = rand.nextInt(1, 100)

      var list = Seq.empty[Pair]

      for(i<-0 until n){
        val k = rand.nextInt(1, MAX_VALUE).toString.getBytes()

         if(!list.exists{case (k1, _) => ord.equiv(k, k1)}){
           list = list :+ k -> k
         }
      }

      index.insert(list).flatMap { case (ok, _) =>

        if(!ok){
          Future.successful(Tuple3(1, false, null))
        } else {

          //We must save first before updating the ref...
          store.save(ctx.blocks).map { ok =>
            Tuple3(1, (ok && root.compareAndSet(old, index.ref)), list)
          }
        }
      }
    }

    val n = rand.nextInt(4, 10)

    var tasks = Seq.empty[Future[(Int, Boolean, Seq[Pair])]]

    for(i<-0 until n){
      tasks = tasks :+ insert()
    }

    val result = Await.result(Future.sequence(tasks), 1 minute)
    val results_ok = result.filter(_._2)

    var data = Seq.empty[Pair]

    results_ok.foreach { case (op, _, list) =>
      data = data ++ list
    }

    val dsorted = data.sorted.map{case (k, _) => new String(k)}
    val ref = root.get()
    val isorted = Query.inOrder(ref.root).map{case (k, _) => new String(k)}

    println(s"dsorted: ${dsorted}\n")
    println(s"isroted: ${isorted}\n")
    println(s"n: ${result.length} succeed: ${results_ok.length} size: ${ref.size}\n")

    assert(dsorted.equals(isorted))
  }

  "index data " should "be equal to test data" in {

    val n = 1

    for(i<-0 until n){
      test()
    }

  }

}
