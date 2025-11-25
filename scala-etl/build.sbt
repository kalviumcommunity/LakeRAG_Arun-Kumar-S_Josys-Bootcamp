ThisBuild / scalaVersion := "2.12.17"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "com.example"

lazy val root = (project in file("."))
  .settings(
    name := "scala-etl",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.4.1",
      "org.apache.spark" %% "spark-sql" % "3.4.1",
      "io.delta" %% "delta-core" % "2.4.0",
      "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.673",
      "org.apache.pdfbox" % "pdfbox" % "2.0.30"
    ),

    fork := true,

    Compile / run / javaOptions ++= Seq(
      "-Dspark.jars.packages=io.delta:delta-core_2.12:2.3.0"
    )
  )

assembly / mainClass := Some("jobs.RawIngestionJob")
assembly / assemblyJarName := "lakerag-etl.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

