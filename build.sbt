name := "magic-rdds"

version := "1.3.2-SNAPSHOT"

libraryDependencies ++= Seq(
  libraries.value('iterators),
  libraries.value('kryo),
  libraries.value('spark_util),
  libraries.value('spire)
)

providedDeps += libraries.value('spark)

testDeps += libraries.value('spark_tests)
