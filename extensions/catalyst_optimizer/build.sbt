name := "ecommerce-catalyst-optimizer"
version := "1.0.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
  "io.delta"         %% "delta-spark" % "3.1.0" % "provided",
  "org.scalatest"    %% "scalatest"  % "3.2.17" % Test
)

// Package as fat JAR for deployment
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
