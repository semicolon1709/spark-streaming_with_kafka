import sbtassembly.AssemblyPlugin.autoImport._
name := "kkboxProject"

version := "0.1"

scalaVersion := "2.11.8"

//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
//  "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
//  "org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided",
//  "org.apache.spark" %% "spark-hive" % "2.2.0" % "provided",
//  "org.apache.spark" %% "spark-yarn" % "2.2.0" % "provided",
//  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0" % "provided",
//  "org.apache.kafka" %% "kafka" % "1.0.2" ,
//  "org.apache.hadoop" % "hadoop-common" % "2.7.3"
//)

//assemblyMergeStrategy in assembly := {
//  case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
//  case x =>
//    val oldStrategy = (assemblyMergeStrategy in assembly).value
//    oldStrategy(x)
//}
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "log4j" % "log4j" % "1.2.16"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-yarn" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "1.0.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.3"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.10"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.10"

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case PathList("org", "jboss", xs@_*) => MergeStrategy.first
  case "about.html" => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.codehaus.jackson.**" -> "shadejackson.@1").inAll
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)