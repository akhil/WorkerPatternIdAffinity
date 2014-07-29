name := "WorkerPatternIdAffinity"

version := "1.0"

scalaVersion := "2.10.4"

publishMavenStyle := true

scalacOptions ++= Seq("-deprecation", "-encoding", "UTF-8", "-feature", "-target:jvm-1.7", "-unchecked",
  "-Ywarn-adapted-args", "-Ywarn-value-discard", "-Xlint", "-language:postfixOps", "-language:existentials", "-language:implicitConversions")

val akkaVersion = "2.3.4"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaVersion

libraryDependencies += "com.typesafe.akka" %% "akka-remote" % akkaVersion
