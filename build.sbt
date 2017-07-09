name := "magic-rdds"

version := "1.5.0-SNAPSHOT"

addSparkDeps

sparkTestsVersion := "2.1.0-SNAPSHOT"
testUtilsVersion := "1.2.4-SNAPSHOT"

libraryDependencies ++= Seq(
  "com.github.alexarchambault" %% "case-app" % "1.2.0-M3",
  libs.value('args4s),
  libs.value('iterators).copy(revision = "1.3.0-SNAPSHOT"),
  libs.value('paths).copy(revision = "1.1.1-SNAPSHOT"),
  libs.value('slf4j),
  libs.value('spark_util).copy(revision = "1.2.0-SNAPSHOT"),
  libs.value('spire)
)
