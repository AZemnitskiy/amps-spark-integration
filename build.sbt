name := "amps-spark-integration"

version := "0.1"

scalaVersion := "2.11.4"


libraryDependencies ++= Seq ("org.apache.spark" %% "spark-streaming" % "2.2.1",
                             "org.apache.spark" %% "spark-core" % "2.2.1")

unmanagedJars in Compile += file(Path.userHome + "/files/libs/amps/amps_client.jar")