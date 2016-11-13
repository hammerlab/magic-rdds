name := "magic-rdds"

version := "1.2.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.spire-math" %% "spire" % "0.11.0",
  "org.scalatest" %% "scalatest" % "3.0.0",
  "org.hammerlab" %% "spark-util" % "1.0.0",
  "org.hammerlab" %% "spark-tests" % "1.0.0" % "test->compile"
)
