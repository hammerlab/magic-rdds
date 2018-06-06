name := "magic-rdds"

v"4.2.1"

addSparkDeps
scalameta

// Skip compilation during doc-generation; otherwise it fails due to macro-annotations not being expanded
emptyDocJar

dep(
             bytes % "1.2.0",
          case_app,
          io_utils % "5.1.0",
         iterators % "2.1.0",
  iterators.macros % "1.0.0",
        math.utils % "2.2.0",
             paths % "1.5.0",
             slf4j,
        spark_util % "2.0.4",
             spire,
             stats % "1.3.0",
             types % "1.1.0",
          parallel % "1.0.0" +testtest
)
