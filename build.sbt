name := "machine-learning"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.7.3",
  "org.apache.spark" %% "spark-sql" % "2.4.7",
  "org.apache.spark" %% "spark-mllib" % "2.4.7",
  "com.databricks" %% "spark-csv" % "1.2.0"
)