import sbt._
import Keys.{console => sbtConsole, _}

object CommonSettings {
  lazy val compiler = Seq(
    scalaVersion := "2.12.6",
    fork := true,

    // Set of "good" compile options from https://tpolecat.github.io/2014/04/11/scalac-flags.html
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-language:existentials",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-unchecked",
      "-Xfatal-warnings",
      "-Xlint",
      "-Yno-adapted-args",
      "-Ypartial-unification",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen",
      "-Ywarn-value-discard",
      "-Xfuture",
      "-Ywarn-unused-import",
      "-Yno-predef"
    ),

    scalacOptions in (Compile, sbtConsole) ~= (_ filterNot (a => a == "-Ywarn-unused-import" || a == "-Xlint")),
    scalacOptions in (Test, sbtConsole) := (scalacOptions in (Compile, sbtConsole)).value,

    resolvers += Resolver.sonatypeRepo("releases"),

    addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.3")
  )

  lazy val console = Seq(
    initialCommands in sbtConsole := """
      import scala.Predef._
      import cats._
      import cats.data._
      import cats.implicits._
      import fandom.search.ingester._
      |""".stripMargin
  )
}
