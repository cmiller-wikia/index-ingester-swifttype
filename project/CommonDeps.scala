import sbt._

object CommonDeps {
  object v {
    val cats = "1.2.0"
    val catsmtl = "0.2.1"
    val catseff = "1.0.0-RC"
    val doobie = "0.4.4"
    val circe = "0.9.3"
    val fs2 = "0.10.5"
    val h2 = "1.4.195"
    val http4s = "0.18.15"
    val logback = "1.2.3"
    val scalacheck = "1.13.4"
    val scalalogging = "3.7.2"
    val scalatest = "3.0.1"
  }

  val cats = Seq (
    "org.typelevel" %% "cats-core" % v.cats,
    "org.typelevel" %% "cats-mtl-core" % v.catsmtl,
    "org.typelevel" %% "cats-effect" % v.catseff,
    "org.typelevel" %% "cats-laws" % v.cats % "test"
  )


  val http4s_common = Seq(
    "org.http4s" %% "http4s-circe" % v.http4s
  )

  val http4s = http4s_common ++ Seq(
    "org.http4s" %% "http4s-dsl"          % v.http4s,
    "org.http4s" %% "http4s-blaze-server" % v.http4s)

  val http4s_client = http4s_common ++ Seq(
    "org.http4s" %% "http4s-client"       % v.http4s,
    "org.http4s" %% "http4s-async-http-client" % v.http4s,
    "org.http4s" %% "http4s-blaze-client" % v.http4s
  )

  val circe = Seq(
    "io.circe" %% "circe-core"    % v.circe,
    "io.circe" %% "circe-generic" % v.circe,
    "io.circe" %% "circe-parser"  % v.circe,
    "io.circe" %% "circe-literal" % v.circe % "test")

  val doobie = Seq(
    "org.tpolecat" %% "doobie-core-cats" % v.doobie,
    "com.h2database" % "h2" % v.h2 % "test"
  )

  val scalatest = Seq(
    "org.scalatest" %% "scalatest"   % v.scalatest % "test",
    "org.scalacheck" %% "scalacheck" % v.scalacheck % "test")

  val logging = Seq(
    "com.typesafe.scala-logging" %% "scala-logging"   % v.scalalogging,
    "ch.qos.logback"              % "logback-classic" % v.logback
  )
}
