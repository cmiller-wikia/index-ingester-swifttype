package fandom.search.ingester

import okhttp3._

object okhttp {
  implicit class RichUrlBuilder(builder: HttpUrl.Builder) {
    def / : String ⇒ HttpUrl.Builder =
      builder.addPathSegment(_)

    def ? : ((String, String)) ⇒ HttpUrl.Builder =
      Function.tupled(builder.addQueryParameter(_, _))
  }
}
