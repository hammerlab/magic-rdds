name := "magic-rdds"
v"4.3.0"

spark

scalameta

// Skip compilation during doc-generation; otherwise it fails due to macro-annotations not being expanded
emptyDocJar

dep(
             bytes % "1.3.0",
          case_app,
          io_utils % "5.2.1",
         iterators % "2.2.0",
  iterators.macros % "1.0.0",
        math.utils % "2.3.0",
             paths % "1.5.0",
             slf4j,
        spark_util % "3.1.0",
             spire,
             stats % "1.3.3",
             types % "1.4.0",
          parallel % "1.0.0" +testtest
)
