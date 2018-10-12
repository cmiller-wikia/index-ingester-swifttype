import scalariform.formatter.preferences._

lazy val root = (project in file("."))
  .settings(CommonSettings.compiler)
  .settings(CommonSettings.console)
  .settings(Seq(
    organization := "fandom.com",
    name := "index-ingester2",
    description := "SwiftType Index Ingester (Experimental)",
    version := "0.5"
  ))
  .settings(Seq(
    libraryDependencies ++=
      CommonDeps.cats ++
      CommonDeps.circe ++
      CommonDeps.scalatest ++
      CommonDeps.logging ++
      Seq(
        "co.fs2" %% "fs2-io" % CommonDeps.v.fs2,
        "io.circe" %% "circe-fs2" % "0.10.0",
        "com.squareup.okhttp3" % "okhttp" % "3.11.0",
        "com.amazonaws" % "aws-java-sdk-s3" % "1.11.390"
      ),
    scalariformPreferences := scalariformPreferences.value
      .setPreference(DanglingCloseParenthesis, Force)
      .setPreference(RewriteArrowSymbols, true)
  ))
