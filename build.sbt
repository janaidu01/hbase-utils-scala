lazy val commonSettings = Seq(
  organization := "com.digitalearns.hbaseutils",
  version := "0.0.1",
  scalaVersion := "2.11.7"
)

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"
  ,"releases" at "http://oss.sonatype.org/content/repositories/releases"
  ,"Clojars Repository" at "http://clojars.org/repo"
  ,"Hadoop Releases" at "https://repository.cloudera.com/content/repositories/releases/"
  ,"cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(

    name := "hbaseutils",

    libraryDependencies := Seq(
      "org.rogach"          %% "scallop"       % "0.9.5"
      , "org.specs2"        %% "specs2-core"   % "2.4.13"
      , "org.apache.hadoop" %  "hadoop-client" % "2.5.0-mr1-cdh5.3.8"
      , "org.apache.hadoop" %  "hadoop-test"   % "2.5.0-mr1-cdh5.3.8" % "test" intransitive
      , "org.apache.hadoop" %  "hadoop-common" % "2.5.0-cdh5.3.8"     % "test" classifier("tests") intransitive
      , "org.apache.hadoop" %  "hadoop-hdfs"   % "2.5.0-cdh5.3.8"     % "test" classifier("tests") intransitive
      , "org.apache.hbase"  %  "hbase"         % "0.98.6-cdh5.3.8"
     )
  )


