name := "magic-rdds"

version := "1.3.3"

addSparkDeps

libraryDependencies ++= Seq(
  libs.value('iterators),
  libs.value('spark_util),
  libs.value('spire)
)
