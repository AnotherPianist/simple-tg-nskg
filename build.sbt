name := "SimpleTG NSKG"

version := "1.0"

scalaVersion := "2.12.15"

scalacOptions ++= Seq("-optimize")

libraryDependencies += "it.unimi.dsi" % "fastutil" % "8.5.9"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.2.1"