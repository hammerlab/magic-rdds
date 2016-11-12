lazy val root =
  (project in file("."))
    .settings(
      name := "magic-rdds",
      version := "1.2.11",
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "1.6.1",
        "org.spire-math" %% "spire" % "0.11.0",
        "args4j" % "args4j" % "2.33",
        "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.4.4"
      )
    )
