package index

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.{Executor, ThreadLocalRandom}

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.google.common.primitives.UnsignedBytes
import com.google.common.util.concurrent.{FutureCallback, Futures}
import org.scalatest.FlatSpec

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, Promise}

class ScyllaDBSpec extends FlatSpec {

  val cluster = Cluster.builder()
  val session = cluster.addContactPoint("localhost").withPort(9042)
    .build().connect("btree")

  val INSERT = new BoundStatement(session.prepare("insert into blocks (key, value) values(?,?)"))
  val SELECT = new BoundStatement(session.prepare("select * from blocks where key = ?"))

  implicit def resultSetFutureToScala(f: ResultSetFuture): Future[ResultSet] = {
    val p = Promise[ResultSet]()
    Futures.addCallback(f, new FutureCallback[ResultSet] {
        def onSuccess(r: ResultSet) = p success r
        def onFailure(t: Throwable) = p failure t
      }, global.asInstanceOf[Executor])

    p.future
  }

  session.execute(QueryBuilder.truncate("blocks"))

  def test(): Unit = {
    println(session)

    val key = UUID.randomUUID.toString.getBytes()
    val value = key

    val f = session.executeAsync(INSERT.bind(ByteBuffer.wrap(key), ByteBuffer.wrap(value))).map { r =>
      println("inserted", r)
    }.flatMap { _ =>
      session.executeAsync(SELECT.bind(ByteBuffer.wrap(key)))
    }.map { r =>
      println("result", new String(r.one().getBytes("value").array()))
    }

    Await.ready(f, 10 seconds)

    session.close()
  }

  "index data " should "be equal to test data" in {

    test()

  }

}
