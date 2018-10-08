package fandom.search.ingester

import io.circe._
import io.circe.syntax._
import com.typesafe.scalalogging._

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
