ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.13.10"


val sparkVersion = "3.3.1"

lazy val root = (project in file("."))
  .settings(
    assembly / mainClass := Some("PageRank"),
    name := "pagerank",
    libraryDependencies += "org.apache.spark" % "spark-sql_2.13" % sparkVersion % Provided,
    libraryDependencies += "org.apache.spark" % "spark-graphx_2.13" % sparkVersion % Provided,
    libraryDependencies += "org.apache.spark" % "spark-core_2.13" % sparkVersion % Provided,
  )
