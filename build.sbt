
lazy val contributors = Seq(
  "ChristopherDavenport" -> "Christopher Davenport"
)

lazy val hadoopVersion = "2.8.1"
lazy val hbaseVersion = "1.1.12"
lazy val scalaTestVersion = "3.0.0"

lazy val kmace = project
  .in(file("."))
  .settings(dontPublishSettings)
  .aggregate(core)

lazy val core = project.in(file("core"))
  .settings(commonSettings)
  .settings(projectSettings)
  .settings(publishingSettings)
  .settings(
    name := "kmace-core"
  )

lazy val projectSettings = Seq(
  organization := "org.http4s",
  licenses := Seq(("Apache 2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),

  scalaVersion := "2.11.11",

  libraryDependencies ++= Seq(
    "org.scalaz.stream" %% "scalaz-stream" % "0.8.5a",
    "org.scalaz" %% "scalaz" % "7.2.16",
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
    "org.apache.hbase" % "hbase-common" % hbaseVersion,
    "org.apache.hbase" % "hbase-client" % hbaseVersion,

//      "org.scalatest" %% "scalatest" % Versions.scalaTest % Test,
//
//// below are set of libraries required for testing HBase with testing server.
//    "org.apache.hadoop" % "hadoop-common" % Versions.hadoop % Test classifier "tests",
//    "org.apache.hadoop" % "hadoop-hdfs" % Versions.hadoop % Test classifier "tests",
//    "org.apache.hadoop" % "hadoop-hdfs" % Versions.hadoop % Test,
//
//    "org.apache.hbase" % "hbase-common" % Versions.hbase % Test classifier "tests",
//    "org.apache.hbase" % "hbase-client" % Versions.hbase % Test classifier "tests",
//    "org.apache.hbase" % "hbase-server" % Versions.hbase % Test,
//    "org.apache.hbase" % "hbase-server" % Versions.hbase % Test classifier "tests",
//    "org.apache.hbase" % "hbase-hadoop-compat" % Versions.hbase % Test,
//    "org.apache.hbase" % "hbase-hadoop2-compat" % Versions.hbase % Test,
//    "org.apache.hbase" % "hbase-hadoop-compat" % Versions.hbase % Test classifier "tests",
//    "org.apache.hbase" % "hbase-hadoop2-compat" % Versions.hbase % Test classifier "tests"
  )

)

/***********************************************************************\
                      Boilerplate below these lines
\***********************************************************************/


lazy val commonSettings = Seq(
  coursierUseSbtCredentials := true,
  coursierChecksums := Nil, // workaround for nexus sync bugs
  addCompilerPlugin("org.spire-math" % "kind-projector" % "0.9.4" cross CrossVersion.binary),

  // Adapted from Rob Norris' post at https://tpolecat.github.io/2014/04/11/scalac-flags.html
  scalacOptions ++= Seq(
    "-deprecation",                      // Emit warning and location for usages of deprecated APIs.
    "-encoding", "utf-8",                // Specify character encoding used by source files.
    "-explaintypes",                     // Explain type errors in more detail.
    "-feature",                          // Emit warning and location for usages of features that should be imported explicitly.
    "-language:existentials",            // Existential types (besides wildcard types) can be written and inferred
    "-language:experimental.macros",     // Allow macro definition (besides implementation and application)
    "-language:higherKinds",             // Allow higher-kinded types
    "-language:implicitConversions",     // Allow definition of implicit functions called views
    "-unchecked",                        // Enable additional warnings where generated code depends on assumptions.
    "-Xcheckinit",                       // Wrap field accessors to throw an exception on uninitialized access.
    "-Xfatal-warnings",                  // Fail the compilation if there are any warnings.
    "-Xfuture",                          // Turn on future language features.
    "-Xlint:adapted-args",               // Warn if an argument list is modified to match the receiver.
    "-Xlint:by-name-right-associative",  // By-name parameter of right associative operator.
//    "-Xlint:constant",                   // Evaluation of a constant arithmetic expression results in an error.
    "-Xlint:delayedinit-select",         // Selecting member of DelayedInit.
    "-Xlint:doc-detached",               // A Scaladoc comment appears to be detached from its element.
    "-Xlint:inaccessible",               // Warn about inaccessible types in method signatures.
    "-Xlint:infer-any",                  // Warn when a type argument is inferred to be `Any`.
    "-Xlint:missing-interpolator",       // A string literal appears to be missing an interpolator id.
    "-Xlint:nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
    "-Xlint:nullary-unit",               // Warn when nullary methods return Unit.
    "-Xlint:option-implicit",            // Option.apply used implicit view.
    "-Xlint:package-object-classes",     // Class or object defined in package object.
    "-Xlint:poly-implicit-overload",     // Parameterized overloaded implicit methods are not visible as view bounds.
    "-Xlint:private-shadow",             // A private field (or class parameter) shadows a superclass field.
    "-Xlint:stars-align",                // Pattern sequence wildcard must align with sequence component.
    "-Xlint:type-parameter-shadow",      // A local type parameter shadows a type already in scope.
    "-Xlint:unsound-match",              // Pattern match may not be typesafe.
    "-Yno-adapted-args",                 // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
    "-Ypartial-unification",             // Enable partial unification in type constructor inference
//    "-Ywarn-dead-code",                  // Warn when dead code is identified.
//    "-Ywarn-extra-implicit",             // Warn when more than one implicit parameter section is defined.
//    "-Ywarn-inaccessible",               // Warn about inaccessible types in method signatures.
//    "-Ywarn-infer-any",                  // Warn when a type argument is inferred to be `Any`.
//    "-Ywarn-nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
//    "-Ywarn-nullary-unit",               // Warn when nullary methods return Unit.
//    "-Ywarn-numeric-widen",              // Warn when numerics are widened.
//    "-Ywarn-unused:implicits",           // Warn if an implicit parameter is unused.
//    "-Ywarn-unused:imports",             // Warn if an import selector is not referenced.
//    "-Ywarn-unused:locals",              // Warn if a local definition is unused.
//    "-Ywarn-unused:params",              // Warn if a value parameter is unused.
//    "-Ywarn-unused:patvars",             // Warn if a variable bound in a pattern is unused.
//    "-Ywarn-unused:privates",            // Warn if a private member is unused.
//    "-Ywarn-value-discard"               // Warn when non-Unit expression results are unused.
  ),

  scalacOptions in (Compile, console) ~= (_.filterNot(Set(
    "-Ywarn-unused:imports",
    "-Xfatal-warnings"
  ))),

  cancelable in Global := true
)

lazy val publishingSettings = Seq(
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (version.value.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  credentials ++= (for {
    username <- Option(System.getenv().get("SONATYPE_USERNAME"))
    password <- Option(System.getenv().get("SONATYPE_PASSWORD"))
  } yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq,
  publishMavenStyle := true,
  pomIncludeRepository := { _ => false },
  pomExtra := {
    <url>https://github.com/http4s/ember</url>
      <developers>
        {for ((username, name) <- contributors) yield
        <developer>
          <id>{username}</id>
          <name>{name}</name>
          <url>http://github.com/{username}</url>
        </developer>
        }
      </developers>
  },
  resolvers += Resolver.mavenLocal
)

lazy val dontPublishSettings = packagedArtifacts := Map.empty



