name := "WikiEditsFlink"

version := "0.1"

scalaVersion := "2.12.12"

// https://mvnrepository.com/artifact/org.apache.flink/flink-table-common
libraryDependencies += "org.apache.flink" % "flink-table-common" % "1.11.2"

// https://mvnrepository.com/artifact/org.apache.flink/flink-table-api-scala-bridge
libraryDependencies += "org.apache.flink" %% "flink-table-api-scala-bridge" % "1.11.2"

// https://mvnrepository.com/artifact/org.apache.flink/flink-connector-wikiedits
libraryDependencies += "org.apache.flink" %% "flink-connector-wikiedits" % "1.11.2"

// https://mvnrepository.com/artifact/org.apache.flink/flink-table-planner-blink
libraryDependencies += "org.apache.flink" %% "flink-table-planner-blink" % "1.11.2"

// https://mvnrepository.com/artifact/org.apache.flink/flink-table-runtime-blink
libraryDependencies += "org.apache.flink" %% "flink-table-runtime-blink" % "1.11.2"

// https://mvnrepository.com/artifact/org.apache.flink/flink-clients
libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.11.2"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"



