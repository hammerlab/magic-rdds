name := "magic-rdds"

version := "2.0.1-SNAPSHOT"

addSparkDeps

deps ++= Seq(
  bytes % "1.0.1",
  case_app,
  io % "1.0.0",
  iterators % "1.3.0",
  math % "1.0.0",
  paths % "1.2.0",
  slf4j,
  spark_util % "1.2.1",
  spire,
  stats % "1.0.0"
)
