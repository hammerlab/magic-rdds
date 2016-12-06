name := "magic-rdds"

version := "1.3.2"

libraryDependencies ++= Seq(
  libraries.value('kryo),
  libraries.value('spark_util),
  libraries.value('spire),
  "org.hammerlab" %% "iterator" % "1.0.0"
)

providedDeps += libraries.value('spark)

testDeps += libraries.value('spark_tests)
