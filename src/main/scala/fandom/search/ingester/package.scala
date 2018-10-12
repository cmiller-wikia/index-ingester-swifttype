package fandom.search

import cats.effect.Sync
import fs2.{ Stream, Pipe }
import scala.util.control.NonFatal
import okhttp3._
import Predef.wrapString
import com.typesafe.scalalogging.Logger
import Function.const

package ingester {
  case class WikiId(val value: String) extends AnyVal

  case class PageId(val value: String) extends AnyVal

  case class WikiInfo(id: WikiId, baseUri: HttpUrl) {
    override def toString: String =
      "%s at %s".format(id, baseUri)
  }

  object util {
    def batchBy[F[_], A](maxBatchSize: Int): Pipe[F, A, List[A]] =
      _.chunkN(maxBatchSize, true).map(_.toList)

    def timestampToIso(ts: Long) = java.time.format.DateTimeFormatter.ISO_INSTANT.format(
      java.time.Instant.ofEpochSecond(ts)
    )

    def skipError[F[_], A](logger: Logger, message: String)(implicit S: Sync[F]): Stream[F, A] =
      Stream.eval(S.delay { logger.error(message) }).flatMap(const(Stream.empty))

    def withResources[T <: AutoCloseable, V](r: ⇒ T)(f: T ⇒ V): V = {
      val resource: T = r
      var exception: Throwable = null
      try {
        f(resource)
      } catch {
        case NonFatal(e) ⇒
          exception = e
          throw e
      } finally {
        closeAndAddSuppressed(exception, resource)
      }
    }

    private def closeAndAddSuppressed(
      e: Throwable,
      resource: AutoCloseable
    ): Unit = {
      if (e != null) {
        try {
          resource.close()
        } catch {
          case NonFatal(suppressed) ⇒
            e.addSuppressed(suppressed)
        }
      } else {
        resource.close()
      }
    }
  }
}
