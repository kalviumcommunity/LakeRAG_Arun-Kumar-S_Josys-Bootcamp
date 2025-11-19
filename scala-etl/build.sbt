ThisBuild / scalaVersion := "2.12.17"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "com.example"

lazy val root = (project in file("."))
  .settings(
    name := "scala-etl",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.4.1",
      "org.apache.spark" %% "spark-sql" % "3.4.1",
      "io.delta" %% "delta-core" % "2.4.0"
    ),

    fork := true
  )
