name := "magic-rdds"

version := "1.4.2"

addSparkDeps

libraryDependencies ++= Seq(
  libs.value('iterators),
  libs.value('paths),
  libs.value('spark_util),
  libs.value('spire)
)
