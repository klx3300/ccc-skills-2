name in ThisBuild := "FD"

version in ThisBuild := "1.0"

scalaVersion in ThisBuild := "2.10.4"

libraryDependencies in ThisBuild += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies in ThisBuild += "org.apache.hadoop" % "hadoop-client" % "2.7.3"
