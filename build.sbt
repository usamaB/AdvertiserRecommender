lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  organization := "org.usama",
  scalaVersion := "2.11.2",
  test in assembly := {}
)

val decline      = "com.monovore" %% "decline" % "1.0.0"
val sparkVersion = "2.4.5"
val sparkDependencies =
  Seq("org.apache.spark" %% "spark-core", "org.apache.spark" %% "spark-sql")
    .map(_ % sparkVersion)

val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % Test
val sparkTestingBase =
  "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % Test excludeAll (
    ExclusionRule("org.scalacheck"),
    ExclusionRule("org.scalactic"),
    ExclusionRule("org.scalatest")
)

resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "AdvertiserRecommender",
    libraryDependencies += scalaTest,
    libraryDependencies ++= sparkDependencies,
    libraryDependencies += decline,
    libraryDependencies += sparkTestingBase
  )

// Memory Requirements for SparkTestingBase
fork in Test := true
javaOptions ++= Seq(
  "-Xms512M",
  "-Xmx2048M",
  "-XX:MaxPermSize=2048M",
  "-XX:+CMSClassUnloadingEnabled"
)
parallelExecution in Test := false
///

//compiler options
scalacOptions ++= Seq(
  "-encoding",
  "utf8",             // Option and arguments on same line
  "-Xfatal-warnings", // New lines for each options
  "-deprecation",
  "-unchecked",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps"
)
