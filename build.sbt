name := "FlinkSkyline"

version := "0.1"

scalaVersion := "2.12.15"

val flinkVersion = "1.16.2"

resolvers += Resolver.mavenCentral

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %  "flink-clients"        % flinkVersion,
  "org.apache.flink" %  "flink-runtime-web"    % flinkVersion,
  "org.apache.flink" %  "flink-connector-kafka"% flinkVersion


)

libraryDependencies += "org.apache.flink" % "flink-connector-kafka" % "3.0.1-1.17"

Compile / run / fork := true
