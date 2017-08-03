name := "magic-rdds"

version := "1.5.0-SNAPSHOT"

addSparkDeps

sparkTestsVersion := "2.1.0-SNAPSHOT"
testUtilsVersion := "1.2.4-SNAPSHOT"

deps ++= Seq(
  case_app,
  cats,
  args4s % "1.2.4-SNAPSHOT",
  iterators % "1.3.0-SNAPSHOT",
  paths % "1.1.1-SNAPSHOT",
  slf4j,
  spark_util % "1.2.0-SNAPSHOT",
  spire
)
