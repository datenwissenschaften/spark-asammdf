name := "spark-asammdf-scala"

version := "0.1"

scalaVersion := "2.13.12"

val sparkVersion = "4.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)

lazy val cleanAll = taskKey[Unit]("Clean all build artifacts")
cleanAll := {
  import scala.sys.process._
  // Process(Seq("rm", "-rf", "build", "dist", "target", "src/python/mdf_spark.egg-info")).!
  Process(Seq("rm", "-rf", "build", "dist", "target", "src/main/python/mdf_spark.egg-info")).!
}

lazy val buildPython = taskKey[Unit]("Build Python with Cython")
buildPython := {
  import scala.sys.process._
  val pythonExec = ".venv/bin/python3" 
  
  // Build extensions in-place for local testing
  val exitCode1 = Process(Seq(pythonExec, "setup.py", "build_ext", "--inplace"), baseDirectory.value).!
  if (exitCode1 != 0) sys.error(s"Python build_ext failed with exit code $exitCode1")
  
  // Build wheel
  val exitCode2 = Process(Seq(pythonExec, "setup.py", "bdist_wheel"), baseDirectory.value).!
  if (exitCode2 != 0) sys.error(s"Python wheel build failed with exit code $exitCode2")
}

lazy val buildAll = taskKey[Unit]("Build Scala and Python")
buildAll := {
  cleanAll.value
  (Compile / packageBin).value
  buildPython.value
}
