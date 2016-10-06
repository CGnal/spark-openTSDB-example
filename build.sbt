import sbt._


val assemblyName = "spark-opentsdb-examples-assembly"

enablePlugins(JavaAppPackaging)

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

val opentsdbExcludes =
  (moduleId: ModuleID) => moduleId.
    excludeAll(ExclusionRule(organization = "org.jboss.netty")).
    excludeAll(ExclusionRule(organization = "org.slf4j")).
    excludeAll(ExclusionRule(organization = "org.apache.spark")).
    excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core"))




val commonDependencies = Seq(

  "org.apache.commons" % "commons-lang3" % commonsLangVersion exclude("org.slf4j", "slf4j-log4j12"),
  "commons-beanutils" % "commons-beanutils" % commonBeanutilsVersion exclude("org.slf4j", "slf4j-log4j12"),
  "log4j" % "log4j" % "1.2.14",
  "org.apache.avro" % "avro" % "1.8.1",
  "com.typesafe" % "config" % "1.0.2",
  ("org.apache.spark" %% "spark-streaming-kafka" % sparkVersion )
    .exclude("org.slf4j", "slf4j-log4j12"),
  "com.twitter" %% "bijection-avro" % "0.9.2",
  "com.twitter" %% "bijection-core" % "0.9.2",
  opentsdbExcludes("com.cgnal.spark" %% "spark-opentsdb" % "1.0"),
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
def providedOrCompileDependencies(scope: String = "compile") = Seq(
  "org.apache.kafka" %% "kafka" % "0.9.0-kafka-2.0.0" % scope,
  hadoopHBaseExcludes("com.databricks" %% "spark-avro" % sparkAvroVersion % scope),
  hadoopHBaseExcludes("org.apache.spark" %% "spark-core" % sparkVersion % scope),
  hadoopHBaseExcludes("org.apache.spark" %% "spark-sql" % sparkVersion % scope),
  hadoopHBaseExcludes("org.apache.spark" %% "spark-mllib" % sparkVersion % scope),
  hadoopHBaseExcludes("org.apache.spark" %% "spark-yarn" % sparkVersion % scope),
  hadoopHBaseExcludes("org.apache.spark" %% "spark-streaming" % sparkVersion % scope),
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
    case x if x.contains("org/jboss/netty/") => MergeStrategy.first
    case x if x.contains("com/google/common/") => MergeStrategy.first
    case x if x.contains("com/fasterxml/jackson/databind/") => MergeStrategy.first
    case x if x.contains("com/fasterxml/jackson/core/") => MergeStrategy.first
    case x if x.contains("com/fasterxml/jackson/annotation/") => MergeStrategy.first
    case x if x.contains("com/esotericsoftware/") => MergeStrategy.first
    case PathList("META-INF", "native",  "libnetty-transport-native-epoll.so") => MergeStrategy.first
    case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
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


/***********************************
  *
  * PACKAGING
  *
  **********************************/

val launcherFilename = "run.sh"

val sparkJobsSimplClassNames = List(
  "com.cgnal.kafkaAvro.consumers.example.OpenTSDBConsumerMain"
)

def addLauncherScriptSh(mainClass: String): String = {
  s"""#!/usr/bin/env bash
./${launcherFilename} ${mainClass}
  """
}

def addBaseLauncherSh: String =
  """#!/usr/bin/env bash

if [ ! -d logs ];
then
  mkdir -p logs
fi


DATE=$(date +%s)
spark-submit --executor-memory 1200M \
  --jars $(JARS=("$(pwd)/lib"/*.jar); IFS=,; echo "${JARS[*]}") \
  --driver-class-path /etc/hbase/conf \
  --conf spark.executor.extraClassPath=/etc/hbase/conf \
  --conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/tmp/jaas.conf \
  --conf spark.driver.extraJavaOptions="-Dspark-opentsdb-exmaples.hbase.master=eligo105.eligotech.private:60000 -Dspark-opentsdb-exmaples.zookeeper.host=eligo105.eligotech.private:2181/kafka -Dspark-opentsdb-exmaples.kafka.brokers=192.168.2.108:9092" \
  --master yarn --deploy-mode client \
  --keytab flanotte.keytab \
  --principal flanotte@SERVER.ELIGOTECH.COM \
  --class com.cgnal.kafkaAvro.consumers.example.OpenTSDBConsumerMain spark-opentsdb-examples_2.10-1.0.0-SNAPSHOT.jar  false flanotte.keytab flanotte@SERVER.ELIGOTECH.COM \
  >  "./logs/${DATE}_OpenTSDBConsumerMain.out" \
  2> "./logs/${DATE}_OpenTSDBConsumerMain.err"

# yarn logs -applicationId=$(getApplicationId "./logs/${DATE}_${CLASSNAME}.err") > "./logs/${DATE}_${CLASSNAME}.executors.log"
                                                                                    """

mappings in Universal += file("target/scripts/run.sh") -> "run.sh"
//mappings in Universal += file("target/scripts/OpenTSDBConsumerMain.sh") -> "OpenTSDBConsumerMain.sh"

/**
  * strip organization in packaged file
  */
mappings in Universal <<= (mappings in Universal ) map { x=>

  val ourPackage: (Seq[(File, String)], Seq[(File, String)]) = x.partition(fileAndMapping=>{
    fileAndMapping._1.getName().endsWith(packagedArtifactName)
  })

  (ourPackage._1.head._1,packagedArtifactName) :: ourPackage._2.toList
}



/**
  * adding launch scripts
  */
val createLaunchScriptsTask  = Def.task {

  val scriptFileDirectory: File = baseDirectory.value / "target" / "scripts"
  scriptFileDirectory.mkdir()

  IO.write( scriptFileDirectory / launcherFilename, addBaseLauncherSh )

// used in case there are multiple main classes
//  sparkJobsSimplClassNames.foreach( x=> {
//    val objName = x.split("\\.").last
//    val scriptFile = scriptFileDirectory / s"${objName}.sh"
//    val content = addLauncherScriptSh(x)
//    IO.write(scriptFile, content)
//  })
}

compile in Compile <<= (compile in Compile) dependsOn createLaunchScriptsTask
packageZipTarball in Universal <<= (packageZipTarball in Universal) dependsOn createLaunchScriptsTask
