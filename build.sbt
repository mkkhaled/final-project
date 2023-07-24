ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "project_spark"
  )
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2"
libraryDependencies +="org.apache.spark" %% "spark-streaming" % "3.3.2"
libraryDependencies += "org.jfree" % "jfreechart" % "1.5.3"
