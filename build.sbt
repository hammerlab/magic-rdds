name := "magic-rdds"

version := "1.4.1"

addSparkDeps

libraryDependencies ++= Seq(
  libs.value('iterators),
  libs.value('spark_util),
  libs.value('spire)
)
