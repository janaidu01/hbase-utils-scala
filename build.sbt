import com.typesafe.sbt.packager.rpm.Keys.linuxPackageMappings

import sbtassembly.AssemblyPlugin.autoImport._

def hadoopVersion      = yourVersion
def hadoopVersionNoMr1 = yourVersion

libraryDependencies :=
    depend.hadoopClasspath ++ depend.hadoop() ++
    Seq(
      "org.rogach"                  %% "scallop"     % "0.9.5",
      "org.specs2"                  %% "specs2-core" % "2.4.13" % "test"
    ) ++
    Seq(
      "org.apache.hadoop" %  "hadoop-test"   % hadoopVersion      % "test" intransitive,
      "org.apache.hadoop" %  "hadoop-common" % hadoopVersionNoMr1 % "test" classifier("tests") intransitive,
      "org.apache.hadoop" %  "hadoop-hdfs"   % hadoopVersionNoMr1 % "test" classifier("tests") intransitive
    )


