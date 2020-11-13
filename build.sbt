import com.typesafe.sbt.packager.docker.Cmd
import com.typesafe.sbt.packager.docker.DockerPermissionStrategy
import com.typesafe.sbt.packager.docker.DockerPermissionStrategy.None

ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "ru.ozon"
ThisBuild / organizationName := "akka_hdfs"

name := "Akka HDFS"

enablePlugins(JavaAppPackaging)

scalaVersion := "2.13.3"

val akkaVersion = "2.6.8"
val alpakkaKafkaVersion = "2.0.4"
val alpakkaHdfsVersion = "2.0.1"

maintainer := "ppenkov@ozon.ru"

libraryDependencies := Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.6.8",
  ("com.lightbend.akka" %% "akka-stream-alpakka-hdfs" % alpakkaHdfsVersion).excludeAll(ExclusionRule(organization = "org.apache.hadoop")),
  "org.apache.hadoop" % "hadoop-client" % "2.9.1",
  "org.apache.hadoop" % "hadoop-common" % "2.9.1",
  "com.typesafe.akka" %% "akka-stream-kafka" % alpakkaKafkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % "1.1.2",
  "commons-codec" % "commons-codec" % "1.12"
)

javaOptions in Universal ++= Seq(
  "-Dcom.sun.management.jmxremote",
  "-Dcom.sun.management.jmxremote.port=9010",
  "-Dcom.sun.management.jmxremote.authenticate=false",
  "-Dcom.sun.management.jmxremote.ssl=false",
  "-J-Xmx2g"
)

javaOptions in run ++= Seq(
  "-Djava.library.path=/usr/lib/x86_64-linux-gnu"
)

fork in run := true

dockerPermissionStrategy in Docker := DockerPermissionStrategy.None

dockerGroupLayers := PartialFunction.empty

mainClass := Some("ru.ozon.akka_hdfs.FileToHdfs")

dockerCommands := Seq(
  Cmd("FROM openjdk:11"),
  Cmd("RUN apt-get update"),
  Cmd("RUN apt-get -y install zlib1g-dev libisal-dev libzstd-dev liblz4-dev"),
  Cmd("WORKDIR /opt/docker"),
  Cmd("COPY opt /opt"),
  Cmd("ENTRYPOINT /opt/docker/bin/file-to-hdfs")
)

