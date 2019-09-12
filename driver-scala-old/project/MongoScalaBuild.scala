/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import scala.util.matching.Regex.Match
import scalariform.formatter.preferences.FormattingPreferences

import com.typesafe.sbt.GitVersioning
import com.typesafe.sbt.SbtScalariform._
import org.scalastyle.sbt.ScalastylePlugin._
import sbt.Keys._
import sbt._
import sbtbuildinfo.BuildInfoKeys.{buildInfoKeys, buildInfoPackage}
import sbtbuildinfo.{BuildInfoKey, BuildInfoPlugin}
import sbtunidoc.Plugin._
import scoverage.ScoverageKeys

object MongoScalaBuild extends Build {

  import Dependencies._
  import Resolvers._

  val baseVersion = "2.8.0"

  val buildSettings = Seq(
    organization := "org.mongodb.scala",
    organizationHomepage := Some(url("http://www.mongodb.org")),
    scalaVersion := scalaCoreVersion,
    crossScalaVersions := scalaVersions,
    libraryDependencies ++= coreDependencies,
    libraryDependencies <+= scalaReflect,
    resolvers := mongoScalaResolvers,
    scalacOptions in Compile := scalacOptionsVersion(scalaVersion.value),
    scalacOptions in Test := scalacOptionsTest,
    scalacOptions in IntegrationTest := scalacOptionsTest,
    // Adds a `src/main/scala-2.13+` source directory for Scala 2.13 and newer
    // and a `src/main/scala-2.13-` source directory for Scala version older than 2.13
    unmanagedSourceDirectories in Compile += {
      val sourceDir = (sourceDirectory in Compile).value
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n >= 13 => sourceDir / "scala-2.13+"
        case _                       => sourceDir / "scala-2.13-"
      }
    }
  )

  val scalacOptionsTest: Seq[String] = Seq( "-unchecked", "-deprecation", "-feature", "-Xlint:-missing-interpolator,_", "-Xcheckinit")

  def scalacOptionsVersion(scalaVersion: String): Seq[String] = {
    Seq( "-unchecked", "-deprecation", "-feature", "-Ywarn-dead-code", "-language:existentials", "-Xlint:-missing-interpolator")
  }

  val versionSettings = Versioning.settings(baseVersion)
  val buildInfoSettings = Seq(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion),
    buildInfoPackage := "org.mongodb.scala"
  )
  val publishSettings = Publish.settings
  val publishAssemblySettings = Publish.publishAssemblySettings
  val noPublishSettings = Publish.noPublishing

  /*
   * Test Settings
   */
  val testSettings = Seq(
    testFrameworks += TestFrameworks.ScalaTest,
    ScoverageKeys.coverageMinimum := 90,
    ScoverageKeys.coverageFailOnMinimum := true,
    libraryDependencies ++= testDependencies
  ) ++ Defaults.itSettings

  lazy val UnitTest = config("unit") extend Test

  val scoverageSettings = Seq()

  /*
   * Style and formatting
   */
  def scalariFormFormattingPreferences: FormattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences()
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
  }

  val customScalariformSettings = scalariformSettings ++ Seq(
    ScalariformKeys.preferences in Compile := scalariFormFormattingPreferences,
    ScalariformKeys.preferences in Test := scalariFormFormattingPreferences
  )

  val scalaStyleSettings = Seq(
    (scalastyleConfig in Compile) := file("project/scalastyle-config.xml"),
    (scalastyleConfig in Test) := file("project/scalastyle-config.xml")
  )

  // Check style
  val checkAlias = addCommandAlias("check", ";clean;scalastyle;coverage;test;it:test;coverageAggregate;coverageReport")

  // Documentation Settings to link to the async JavaDoc
  val driverDocsVersion = mongodbDriverVersion.split("\\.").dropRight(1).mkString(".")
  val mongodbApiURL = s"http://mongodb.github.io/mongo-java-driver/$driverDocsVersion/javadoc"
  val scalaApiURL = s"http://scala-lang.org/api/$scalaCoreVersion"
  val docSettings = Seq(
    autoAPIMappings := true,
    apiMappings ++= {
      def findManagedDependency(organization: String, name: String): Option[File] = {
        (for {
          entry <- (fullClasspath in Runtime).value ++ (fullClasspath in Test).value
          module <- entry.get(moduleID.key) if module.organization == organization && module.name.startsWith(name)
        } yield entry.data).headOption
      }
      val links = Seq(
        findManagedDependency("org.mongodb", "mongodb-driver-async").map(d => d -> url(mongodbApiURL)),
        findManagedDependency("org.mongodb", "mongodb-driver-core").map(d => d -> url(mongodbApiURL)),
        findManagedDependency("org.mongodb", "bson").map(d => d -> url(mongodbApiURL)),
        findManagedDependency("org.scala-lang", "scala-library").map(d => d -> url(scalaApiURL))
      )
      links.collect { case Some(d) => d }.toMap
    }
  ) ++ fixJavaLinksSettings

  lazy val fixJavaLinks = taskKey[Unit]("Fix Java links in scaladoc - replace #java.io.File with ?java/io/File.html" )
  lazy val fixJavaLinksSettings = fixJavaLinks := {
    val t = (target in UnidocKeys.unidoc).value
    (t ** "*.html").get.filter(hasJavadocApiLink).foreach { f =>
      val newContent = javadocApiLink.replaceAllIn(IO.read(f), fixJavaLinksMatch)
      IO.write(f, newContent)
    }
  }
  val fixJavaLinksMatch: Match => String = m => m.group(1) + "?" + m.group(2).replace(".", "/") + ".html"
  val javadocApiLink = List("\"(\\Q", mongodbApiURL, "index.html\\E)#([^\"]*)\"").mkString.r
  def hasJavadocApiLink(f: File): Boolean = (javadocApiLink findFirstIn IO.read(f)).nonEmpty

  val rootUnidocSettings = Seq(
    scalacOptions in(Compile, doc) ++= Opts.doc.title("Mongo Scala Driver"),
    scalacOptions in(Compile, doc) ++= Seq("-diagrams", "-unchecked", "-doc-root-content", "rootdoc.txt")
  ) ++ docSettings ++ unidocSettings ++ Seq(fixJavaLinks <<= fixJavaLinks triggeredBy (doc in ScalaUnidoc))

  lazy val driver = Project(
    id = "mongo-scala-driver",
    base = file("driver")
  ).configs(IntegrationTest)
    .configs(UnitTest)
    .settings(buildSettings)
    .settings(buildInfoSettings)
    .settings(testSettings)
    .settings(customScalariformSettings)
    .settings(scalaStyleSettings)
    .settings(scoverageSettings)
    .settings(docSettings)
    .settings(publishSettings)
    .settings(publishAssemblySettings)
    .dependsOn(bson)
    .enablePlugins(GitVersioning, BuildInfoPlugin)

  lazy val bson = Project(
    id = "mongo-scala-bson",
    base = file("bson")
  ).configs(IntegrationTest)
    .configs(UnitTest)
    .settings(buildSettings)
    .settings(versionSettings)
    .settings(testSettings)
    .settings(scalaStyleSettings)
    .settings(docSettings)
    .settings(publishSettings)
    .enablePlugins(GitVersioning, BuildInfoPlugin)

  lazy val examples = Project(
    id = "mongo-scala-driver-examples",
    base = file("examples")
  ).configs(UnitTest)
    .aggregate(bson)
    .aggregate(driver)
    .settings(buildSettings)
    .settings(scalaStyleSettings)
    .settings(noPublishSettings)
    .settings(libraryDependencies ++= examplesDependencies)
    .dependsOn(driver)

  lazy val root = Project(
    id = "mongo-scala",
    base = file(".")
  ).aggregate(bson)
    .aggregate(driver)
    .aggregate(examples)
    .settings(buildSettings)
    .settings(scalaStyleSettings)
    .settings(scoverageSettings)
    .settings(rootUnidocSettings)
    .settings(noPublishSettings)
    .settings(checkAlias)
    .settings(initialCommands in console := """import org.mongodb.scala._""")
    .dependsOn(driver)

  override def rootProject: Some[Project] = Some(root)

}
