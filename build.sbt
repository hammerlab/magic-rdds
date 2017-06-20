name := "magic-rdds"

version := "1.4.3"

addSparkDeps

libraryDependencies ++= Seq(
  libs.value('iterators),
  libs.value('paths),
  libs.value('slf4j),
  libs.value('spark_util),
  libs.value('spire)
)
