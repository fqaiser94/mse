name := "mse"

version := "0.1.2"

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

licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

parallelExecution in Test := false

ThisBuild / githubOwner := "fqaiser94"
ThisBuild / githubRepository := "mse"
ThisBuild / githubUser := sys.env.getOrElse("GITHUB_USER", "")
ThisBuild / githubTokenSource := Some(TokenSource.Environment("GITHUB_TOKEN"))