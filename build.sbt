name := "magic-rdds"

version := "2.2.0-SNAPSHOT"

addSparkDeps

deps ++= Seq(
  bytes % "1.0.2",
  case_app,
  io % "1.1.0",
  iterators % "1.3.1-SNAPSHOT",
  math % "1.0.0",
  paths % "1.2.0",
  slf4j,
  spark_util % "1.3.0",
  spire,
  stats % "1.0.1"
)
