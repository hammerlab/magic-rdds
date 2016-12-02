name := "magic-rdds"

version := "1.3.1"

libraryDependencies ++= Seq(
  libraries.value('spire),
  "org.hammerlab" %% "iterator" % "1.0.0",
  "org.hammerlab" %% "spark-util" % "1.1.1"
)

providedDeps += libraries.value('spark)

testDeps += "org.hammerlab" %% "spark-tests" % "1.1.2"
