package io.github.easel

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Framing, Sink, Source}
import akka.testkit.TestKit
import akka.util.ByteString
import org.scalatest.{AsyncWordSpec, AsyncWordSpecLike, MustMatchers}

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.duration._

class RecursiveAkkaHttpSpec
  extends TestKit(ActorSystem("test"))
    with AsyncWordSpecLike
    with MustMatchers {
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  val years = 2016 to 2017 //1997 to 2017
  val quarters = 3 to 4
  val searchUrls = Seq(
    "https://www.google.com",
    "https://www.yahoo.com",
    "https://wikipedia.org",
    "https://www.amazon.com")
  val secUrls = for (year <- years; quarter <- quarters)
    yield
      s"https://www.sec.gov/Archives/edgar/full-index/$year/QTR$quarter/master.idx"

  val allUrls = Seq(
    (searchUrls, 900000, 11000),
    (secUrls, 973518, 17880456)
  )

  lazy val pool = Http().superPool[String]()

  val streamingRequestFlow: Flow[HttpRequest, ByteString, NotUsed] =
    Flow[HttpRequest]
      .map(x => (x, x.uri.toString))
      .via(pool)
      .flatMapConcat {
        case (Success(response), uri) =>
          response.entity match {
            case e: HttpEntity.Chunked =>
              println(s"$uri is CHUNKED")
            case _ =>
              println(s"$uri is NOT CHUNKED")
          }
          response.entity.dataBytes
        case (Failure(e), uri) =>
          throw (e)
      }

  val strictRequestFlow: Flow[HttpRequest, ByteString, NotUsed] =
    Flow[HttpRequest]
      .map(x => (x, x.uri.toString))
      .via(pool)
      .mapAsync(1) {
        case (Success(response), _) =>
          response.entity.toStrict(10.seconds).map(_.data)
        case (Failure(e), _) =>
          Future.failed(e)
      }

  val parseFlow: Flow[ByteString, String, NotUsed] = Flow[ByteString]
    .via(
      Framing.delimiter(ByteString("\n"),
        maximumFrameLength = Integer.MAX_VALUE,
        allowTruncation = true))
    .map(_.utf8String)

  "akka http" should {
    allUrls.foreach { case (urls, combinedSize, firstSize) =>
      val first = urls.head
      val requests = urls.map { url =>
        HttpRequest(uri = url)
      }

      s"request a single file from $first and combine its lines as a stream" in {
        Source
          .fromIterator(() => requests.iterator.take(1))
          .via(streamingRequestFlow)
          .via(parseFlow)
          .runFold(0)(_ + _.size)
          .map { s =>
            s must be >= firstSize
          }
      }

      s"request the files starting at $first and combine their lines as a stream" in {
        Source
          .fromIterator(() => requests.iterator)
          .via(streamingRequestFlow)
          .via(parseFlow)
          .runFold(0)(_ + _.size)
          .map { s =>
            s must be >= combinedSize
          }
      }
      s"request the files starting at $first and combine their lines via strict responses" ignore {
        Source
          .fromIterator(() => requests.iterator)
          .via(strictRequestFlow)
          .via(parseFlow)
          .runFold(0)(_ + _.size)
          .map { s =>
            s mustEqual combinedSize
          }
      }
    }
  }
}
