name := "ScalaSparkProjects"

version := "1.0"

scalaVersion := "2.12.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-mllib" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "log4j" % "log4j" % "1.2.17"
)
