name:= "spark-blog-examples"

val sparkVersion = "1.6.1"
val cassandraVersion = "1.4.0"

scalaVersion := "2.11.7"

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

libraryDependencies++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
	"com.datastax.spark" %% "spark-cassandra-connector" % cassandraVersion
	)
