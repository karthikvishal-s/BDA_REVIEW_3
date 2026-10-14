// Pinned to the Spark build inside the Docker image: Spark 3.5.3 / Scala 2.12.
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "1.0.0"

lazy val sparkVersion = "3.5.3"

lazy val root = (project in file("."))
  .settings(
    name := "review3-flight-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
    ),
    // Fixed JAR name so the Makefile does not depend on the version string.
    Compile / packageBin / artifactPath := baseDirectory.value / "target" / "review3-flight-spark.jar",
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")
  )
