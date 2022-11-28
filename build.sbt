name:="HelloSpark"
organization:="learning.spark"
version:="0.1"
//scalaVersion:="2.12.10"
scalaVersion:="2.12.0"
autoScalaLibrary:=false
val sparkVersion="3.0.0-preview2"
//val sparkVersion="2.11.0"

val sparkDependencies = Seq(
  "org.apache.spark"%%"spark-core"%sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
  //"com.microsoft.azure.synapse" % "synapseutils_2.11" % "1.2"
)
val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)
libraryDependencies ++= sparkDependencies ++ testDependencies
/*libraryDependencies += "com.azure" % "azure-storage-file-datalake" % "12.8.0"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.0"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.10.0"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.10.0"
dependencyOverrides += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % "2.10.0"
dependencyOverrides += "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.10.0"
dependencyOverrides += "io.netty" % "netty-all" % "4.1.73.final" exclude("io.netty","netty-tcnative")*/

