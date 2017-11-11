name := "magic-rdds"

version := "4.0.0-SNAPSHOT"

addSparkDeps
scalameta

deps ++= Seq(
  bytes % "1.0.3",
  case_app,
  io % "3.0.0",
  iterators % "2.0.0",
  math % "2.1.1",
  hammerlab("iterator-macros") % "1.1.0",
  paths % "1.3.1",
  slf4j,
  spark_util % "2.0.0",
  spire,
  stats % "1.1.1",
  types % "1.0.1"
)
