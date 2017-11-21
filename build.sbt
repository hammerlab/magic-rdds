name := "magic-rdds"

version := "4.0.1-SNAPSHOT"

addSparkDeps
scalameta

// Skip compilation during doc-generation; otherwise it fails due to macro-annotations not being expanded
emptyDocJar

deps ++= Seq(
  bytes % "1.1.0",
  case_app,
  io % "3.1.0",
  iterator_macros % "1.1.0",
  iterators % "2.0.0",
  math % "2.1.1",
  paths % "1.4.0",
  slf4j,
  spark_util % "2.0.1",
  spire,
  stats % "1.1.1",
  types % "1.0.1"
)
