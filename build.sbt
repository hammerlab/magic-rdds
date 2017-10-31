name := "magic-rdds"

version := "4.0.0-SNAPSHOT"

addSparkDeps

deps ++= Seq(
  bytes % "1.0.3",
  case_app,
  io % "2.0.0",
  iterators % "1.4.0",
  math % "2.0.0",
  monoids % "1.0.0-SNAPSHOT",
  paths % "1.3.1",
  slf4j,
  spark_util % "2.0.0",
  spire,
  stats % "1.0.1"
)
