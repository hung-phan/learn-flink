name := "learn-flink"

version := "0.1"

scalaVersion := "2.12.7"

lazy val flinkVersion = "1.10.1"

lazy val root = (project in file("."))
    .settings(
      libraryDependencies ++= Seq(
        "org.apache.flink" %% "flink-scala" % flinkVersion,
        "org.apache.flink" %% "flink-streaming-scala" % flinkVersion
      )
    )

onLoad in Global ~= (_ andThen ("project root" :: _))
