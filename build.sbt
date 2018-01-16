
name := "amps-spark-integration"

version := "0.1"

scalaVersion := "2.11.4"


libraryDependencies ++= Seq ("org.apache.spark" %% "spark-streaming" % "2.2.1" % "provided",
                             "org.apache.spark" %% "spark-core" % "2.2.1" % "provided")

unmanagedJars in Compile += file(Path.userHome + "/files/libs/amps/amps_client.jar")

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}