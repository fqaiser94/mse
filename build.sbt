name := "mse"

scalaVersion := "2.12.10"

crossScalaVersions := Seq("2.11.12", "2.12.10")

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.1" % Test
)

parallelExecution in Test := false

inThisBuild(List(
  organization := "com.github.fqaiser94",
  homepage := Some(url("https://github.com/fqaiser94/mse")),
  licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  developers := List(Developer("fqaiser94", "fqaiser94", "", url("https://github.com/fqaiser94")))
))