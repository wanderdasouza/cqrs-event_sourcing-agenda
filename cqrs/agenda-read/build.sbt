val finchVersion = "0.26.0"
val circeVersion = "0.10.1"
val scalatestVersion = "3.0.5"

lazy val root = (project in file("."))
  .settings(
    organization := "br.usp",
    name := "agenda-read",
    version := "0.0.1-SNAPSHOT",
    scalaVersion := "2.12.14",
    libraryDependencies ++= Seq(
      "com.github.finagle" %% "finchx-core"  % finchVersion,
      "com.github.finagle" %% "finchx-circe"  % finchVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "org.scalatest"      %% "scalatest"    % scalatestVersion % "test",
      "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0",
      "org.apache.kafka" %% "kafka" % "2.1.0"
    )
  )