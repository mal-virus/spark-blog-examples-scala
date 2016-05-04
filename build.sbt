name:= "hands-on-spark-ml"

val sparkVersion = "1.5.0"
val sparkMLVersion = "1.5.0"
val cassandraVersion = "1.4.0"

scalaVersion := "2.10.4"

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

libraryDependencies++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-mllib" % sparkMLVersion % "provided",
	"com.datastax.spark" %% "spark-cassandra-connector" % cassandraVersion
	)
