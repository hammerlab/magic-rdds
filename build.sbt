name := "magic-rdds"

version := "1.5.0-SNAPSHOT"

addSparkDeps

libraryDependencies ++= Seq(
  libs.value('iterators).copy(revision = "1.3.0-SNAPSHOT"),
  libs.value('paths),
  libs.value('slf4j),
  libs.value('spark_util),
  libs.value('spire)
)
