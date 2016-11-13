name := "magic-rdds"

version := "1.2.11-SNAPSHOT"

libraryDependencies <++= libraries { v => Seq(
  v('spark),
  "org.spire-math" %% "spire" % "0.11.0",
  "org.hammerlab" %% "spark-util" % "1.0.0",
  "org.hammerlab" %% "spark-tests" % "1.0.0" % "test"
)}
