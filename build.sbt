name := "learn-flink"

version := "0.1"

scalaVersion := "2.12.7"

lazy val flinkVersion = "1.10.1"

lazy val root = (project in file("."))
    .settings(
      libraryDependencies ++= Seq(
        "org.apache.flink" %% "flink-scala" % flinkVersion,
        "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
        "org.apache.flink" %% "flink-connector-kafka" % "1.11.2",
        "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.1"
      )
    )

onLoad in Global ~= (_ andThen ("project root" :: _))
