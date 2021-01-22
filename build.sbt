import sbt.Keys.libraryDependencies

name := "CS441-HW2"

version := "0.1"

scalaVersion := "2.13.3"
scalacOptions += "-target:jvm-1.8"
mainClass in assembly := Some("ScalaCode.MapReduceDriver")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(

  "com.typesafe" % "config" % "1.4.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "junit" % "junit" % "4.13" % "test",

  "org.scala-lang.modules" %% "scala-xml" % "1.3.0",

  // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-core
  "org.apache.hadoop" % "hadoop-core" % "1.2.1",

  // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
//  "org.apache.hadoop" % "hadoop-common" % "3.2.1",

  // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core
//  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.2.1",

  // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
//  "org.apache.hadoop" % "hadoop-client" % "3.2.1"

)


