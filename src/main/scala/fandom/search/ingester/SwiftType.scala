package fandom.search.ingester

import io.circe._
import io.circe.syntax._

import _root_.fs2.Stream
import okhttp3._
import okhttp._
import io.circe._
import io.circe.parser.decode
import io.circe.generic.semiauto._
import cats.effect._
import com.typesafe.scalalogging._

case class IngestResponse(id: Option[String], errors: List[String]) {
  def toResult: IngestResult =
    if (errors.isEmpty) IngestSuccess(id) else RejectedDocument(id, errors)
}

case class SwiftDocument(doc: JsonObject) extends AnyVal {
  def id: Option[String] =
    doc("id").flatMap(_.asString)
}

case object SwiftDocument {
  val logger = Logger("MainLog")
  def from(obj: JsonObject) = SwiftDocument(shrink(JsonObject.fromIterable(
    obj.toIterable.map {
      case ("objectID", oid) ⇒ ("id", oid)
      case ("last_updated", lastUpdated) ⇒ ("last_updated", lastUpdated.asNumber.flatMap(_.toLong).map(util.timestampToIso(_)).map(_.asJson).getOrElse(lastUpdated))
      case x ⇒ x
    }
  )))

  def truncate(in: String): String = {
    var s: String = in.substring(0, Math.min(in.length, 90000))

    while (s.getBytes("UTF-8").length > 90000) {
      s = s.substring(0, Math.round(s.length.toFloat * 0.9f))
    }

    s
  }

  def shrink(obj: JsonObject): JsonObject = {
    val id = obj("id")
    val size = obj.asJson.noSpaces.getBytes("UTF-8").length
    if (size > 90000) {
      logger.error("Truncating too-large page " + id.toString)
      JsonObject.fromIterable(
        obj.toIterable.map {
          case ("content", content) ⇒
            ("content", Json.fromString(truncate(content.asString.getOrElse(""))))
          case x ⇒ x
        }
      )
    } else {
      obj
    }
  }
}

sealed trait IngestResult

case class IngestSuccess(id: Option[String]) extends IngestResult
case class HttpError(id: Option[String], reason: String) extends IngestResult
case class BadResponse(id: Option[String], reason: String) extends IngestResult
case class RejectedDocument(id: Option[String], reasons: List[String]) extends IngestResult

class SwiftType(val engineName: String, val apiUri: HttpUrl, val apiKey: String) {
  val logger = Logger("MainLog")
  implicit val responseDecoder: Decoder[IngestResponse] = deriveDecoder[IngestResponse]
  implicit val documentEncoder: Encoder[SwiftDocument] = Encoder.encodeJsonObject.contramap(_.doc)

  def index(client: OkHttpClient)(objects: List[JsonObject]): Stream[IO, IngestResult] = {
    // Submit a batch, then if the batch fails try each doc one by one.
    def submit(docs: List[SwiftDocument]): Stream[IO, IngestResult] = docs match {
      case Nil ⇒ Stream.empty
      case d :: Nil ⇒
        streamRequest(
          client,
          docs,
          httpErr ⇒ Stream.emit(HttpError(d.id, httpErr.toString)),
          jsonErr ⇒ Stream.emit(BadResponse(d.id, jsonErr.toString))
        )
      case _ ⇒
        streamRequest(
          client,
          docs,
          err ⇒ { logger.error("batch failed" + err.toString); submitIndividually(docs) },
          err ⇒ { logger.error("batch failed: " + err.toString); submitIndividually(docs) }
        )
    }

    def submitIndividually(ids: List[SwiftDocument]): Stream[IO, IngestResult] =
      ids match {
        case Nil ⇒ Stream.empty
        case d :: Nil ⇒ submit(List(d))
        case d :: ds ⇒ submit(List(d)) ++ submitIndividually(ds)
      }

    submit(objects.map(SwiftDocument.from))
  }

  val addDocumentUri = (
    apiUri.newBuilder() / "api" / "as" / "v1" / "engines" / engineName / "documents"
  ).build()

  val MEDIA_JSON = MediaType.parse("application/json")

  def request(documents: List[Json]): Request = {
    new Request.Builder()
      .post(RequestBody.create(MEDIA_JSON, Json.fromValues(documents).noSpaces))
      .addHeader("Authorization", "Bearer " + apiKey)
      .url(addDocumentUri)
      .build()
  }

  def streamRequest(
    client: OkHttpClient,
    docs: List[SwiftDocument],
    onHttpError: Throwable ⇒ Stream[IO, IngestResult],
    onDecodeError: Error ⇒ Stream[IO, IngestResult]
  ): Stream[IO, IngestResult] =
    Stream.eval(performRequest(client, docs))
      .map(decode[List[IngestResponse]])
      .flatMap {
        case Left(err) ⇒ onDecodeError(err)
        case Right(succ) ⇒ Stream.emits(succ.map(_.toResult))
      }
      .handleErrorWith(onHttpError)

  def performRequest(client: OkHttpClient, documents: List[SwiftDocument]): IO[String] =
    IO {
      util.withResources(client.newCall(request(documents.map(_.asJson))).execute())(_.body.string)
    }
}

object SwiftType {
  val logger = Logger("MainLog")
  def apply(engineName: String, apiUri: HttpUrl, apiKey: String) =
    new SwiftType(engineName, apiUri, apiKey)
}
