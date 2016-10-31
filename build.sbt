lazy val root =
  (project in file("."))
    .settings(
      organization := "org.hammerlab",
      name := "magic-rdds",
      version := "1.2.9",
      scalaVersion := "2.11.8",
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "1.6.1",
        "org.spire-math" %% "spire" % "0.11.0",
        "args4j" % "args4j" % "2.33",
        "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.4.4"
      )
    )

parallelExecution in Test := false

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }

pomExtra :=
  <url>
    https://github.com/hammerlab/magic-rdds
  </url>
    <licenses>
      <license>
        <name>Apache License</name>
        <url>https://raw.github.com/hammerlab/magic-rdds/master/LICENSE</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:hammerlab/magic-rdds.git</url>
      <connection>scm:git:git@github.com:hammerlab/magic-rdds.git</connection>
      <developerConnection>scm:git:git@github.com:hammerlab/magic-rdds.git</developerConnection>
    </scm>
    <developers>
      <developer>
        <id>hammerlab</id>
        <name>Hammer Lab</name>
        <url>https://github.com/hammerlab</url>
      </developer>
    </developers>
