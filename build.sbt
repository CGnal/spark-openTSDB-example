import sbt._


val assemblyName = "spark-opentsdb-examples-assembly"


import sbt.Keys._

sbtVersion := "0.13.11"

val cdhRelease = "cdh5.7.1"
val sparkVersion = s"1.6.0-${cdhRelease}"
val hbaseVersion = s"1.2.0-${cdhRelease}"
val hadoopVersion = s"2.6.0-${cdhRelease}"
val solrVersion = s"4.10.3-${cdhRelease}"
val sparkAvroVersion = s"1.1.0-${cdhRelease}"
val scalaVersionString = "2.10"
val sparkTSVersion = "0.3.0"
val log4jVersion = "1.2.17"
val slf4jVersion = "1.7.21"
val tikaVersion = "1.12"
val sparkCSVVersion = "1.4.0"
val commonsLangVersion = "3.0"
val scalaTestVersion = "3.0.0"
val commonBeanutilsVersion: String = "1.8.0"
val guavaVersion: String = "14.0.1"
val staxApi: String = "1.0-2"
val metricsCoreVersion: String = "3.1.2"

val jacksonMapperVersion: String = "1.9.0"

val ourOrganization = "com.intesasanpaolo.bip"
val ourVersion = "1.0.0-SNAPSHOT"
val ourArtifactName = "spark-opentsdb-examples"

val packagedArtifactName = ourArtifactName + "_" + scalaVersionString + "-" + ourVersion + ".jar"


def commonSettings(moduleName: String) = Seq(
  name := moduleName,
  organization := "com.intesasanpaolo.bip",
  version := "1.0.0-SNAPSHOT",
  scalaVersion := "2.10.6",
  javaOptions += "-Xms512m -Xmx2G",
  resolvers ++= Seq(
    Resolver.mavenLocal,
    "Cloudera CDH" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  ),
  scalacOptions ++= Seq(
    /*"-deprecation",
    "-encoding", "UTF-8", // yes, this is 2 args
    "-feature",
    "-unchecked",
    "-Xfatal-warnings",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Xfuture" */
  )
)

/**
  * unless Spark and hadoop get in  trouble about signed jars.
  */
val hadoopHBaseExcludes =
(moduleId: ModuleID) => moduleId.
  excludeAll(ExclusionRule(organization = "org.mortbay.jetty")).
  excludeAll(ExclusionRule(organization = "javax.servlet"))



val commonDependencies = Seq(

  "org.apache.commons" % "commons-lang3" % commonsLangVersion,
  "commons-beanutils" % "commons-beanutils" % commonBeanutilsVersion,
  /*"com.google.guava" % "guava" % guavaVersion force(),
  "org.codehaus.jackson" % "jackson-mapper-asl" % jacksonMapperVersion force(),
  "javax.xml.stream" % "stax-api" % staxApi,
  "io.dropwizard.metrics" % "metrics-core" % metricsCoreVersion,*/
  "com.gensler" %% "scalavro" % "0.6.2",

  hadoopHBaseExcludes("org.apache.spark" %% "spark-streaming-kafka" % sparkVersion),
  "com.cgnal.spark" %% "spark-opentsdb" % "1.0" ,
  //"org.apache.kafka" %% "kafka" % "0.9.0.1" exclude(slf4jLog4jOrg, slf4jLog4jArtifact),


  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "org.apache.hbase" % "hbase-testing-util" % hbaseVersion % "test",
  hadoopHBaseExcludes("com.cloudera.sparkts" % "sparkts" % sparkTSVersion ),

  /**
    * all of these are required to test hadoop.
    */
  hadoopHBaseExcludes("org.apache.hbase" % "hbase-server" % hbaseVersion % "test" classifier "tests"),
  hadoopHBaseExcludes("org.apache.hbase" % "hbase-server" % hbaseVersion % "test" extra "type" -> "test-jar"),
  hadoopHBaseExcludes("org.apache.hbase" % "hbase-common" % hbaseVersion % "test" classifier "tests"),
  hadoopHBaseExcludes("org.apache.hbase" % "hbase-testing-util" % hbaseVersion % "test" classifier "tests"),
  hadoopHBaseExcludes("org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion % "test" classifier "tests"),
  hadoopHBaseExcludes("org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion % "test" classifier "tests" extra "type" -> "test-jar"),
  hadoopHBaseExcludes("org.apache.hbase" % "hbase-hadoop2-compat" % hbaseVersion % "test" classifier "tests" extra "type" -> "test-jar"),
  hadoopHBaseExcludes("org.apache.hadoop" % "hadoop-client" % hadoopVersion % "test" classifier "tests"),
  hadoopHBaseExcludes("org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "test" classifier "tests"),
  hadoopHBaseExcludes("org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "test" classifier "tests" extra "type" -> "test-jar"),
  hadoopHBaseExcludes("org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "test" extra "type" -> "test-jar"),
  hadoopHBaseExcludes("org.apache.hadoop" % "hadoop-client" % hadoopVersion % "test" classifier "tests"),
  hadoopHBaseExcludes("org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion % "test"),
  hadoopHBaseExcludes("org.apache.hadoop" % "hadoop-common" % hadoopVersion % "test" classifier "tests" extra "type" -> "test-jar"),
  hadoopHBaseExcludes("org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion % "test" classifier "tests")
)

/**
  * when used inside the IDE they are imported with scope "compile",
  * Otherwise when submitted with spark_submit they are  "provided"
  */
def providedOrCompileDependencies(scope: String = "provided") = Seq(
  hadoopHBaseExcludes("com.databricks" %% "spark-avro" % sparkAvroVersion % scope),
  hadoopHBaseExcludes("org.apache.spark" %% "spark-core" % sparkVersion % scope),
  hadoopHBaseExcludes("org.apache.spark" %% "spark-sql" % sparkVersion),
  hadoopHBaseExcludes("org.apache.spark" %% "spark-mllib" % sparkVersion % scope),
  hadoopHBaseExcludes("org.apache.spark" %% "spark-yarn" % sparkVersion % scope),
  hadoopHBaseExcludes("org.apache.spark" %% "spark-streaming" % sparkVersion),
  hadoopHBaseExcludes("org.apache.hbase" % "hbase-common" % hbaseVersion % scope),
  hadoopHBaseExcludes("org.apache.hbase" % "hbase-client" % hbaseVersion % scope),
  hadoopHBaseExcludes("org.apache.hbase" % "hbase-server" % hbaseVersion % scope),
  hadoopHBaseExcludes("org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion % scope)
)

/**
  * working job
  */
lazy val sparkJobs: Project = (project in file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*)
  .settings(commonSettings(ourArtifactName): _*)
  .settings(
    libraryDependencies ++= commonDependencies ++ providedOrCompileDependencies()
    //dependencyOverrides += "com.google.guava" % "guava" % guavaVersion
    //dependencyOverrides += "com.google.guava" % "guava" % guavaVersion
  ).settings(
  assemblyMergeStrategy in assembly := {
    case PathList(ps@_*) if ps.last endsWith "package-info.class" => MergeStrategy.first
    case PathList("org", "apache", "commons", "collections", x) => MergeStrategy.first
    case PathList("org", "apache", "commons", "logging", x) => MergeStrategy.first
    case PathList("org", "apache", "commons", "logging", "impl", x) => MergeStrategy.first
    case "org/apache/spark/unused/UnusedStubClass.class" => MergeStrategy.last
    case "log4j.properties" => MergeStrategy.concat
    case "com/google/common/base/Stopwatch$1.class" => MergeStrategy.last
    case "com/google/common/base/Stopwatch.class" => MergeStrategy.last
    case "com/google/common/io/Closeables.class" => MergeStrategy.last
    case "com/google/common/io/NullOutputStream.class" => MergeStrategy.last
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
  ,
  assemblyJarName in assembly := s"${name.value}-${version.value}.jar"
)

/**
  * to be used in the IDE
  */
lazy val mainRunner = project
  .in(file("mainRunner"))
  .dependsOn(sparkJobs)
  .settings(commonSettings("mainRunner"): _*)
  .settings(
    libraryDependencies ++= commonDependencies ++ providedOrCompileDependencies("compile")
  )

/**
  * according to
  * http://stackoverflow.com/questions/18838944/how-to-add-provided-dependencies-back-to-run-test-tasks-classpath/21803413#21803413
  * this is needed to reimport back all provided dependencies in test scope.
  */
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

/**
  * assembly is ins a trouble with tests, as the are executed concurrently,
  * so they are simply disabled.
  */
test in assembly := {}

fork in test := true
parallelExecution in Test := false





