// SBT build file for single module application

def initProject(project: Project) : Project = project
  .configure(initSourceDirs)
  .settings(
    name := project.base.getAbsoluteFile.name,
    testOptions in Test := Seq(Tests.Argument(TestFrameworks.JUnit, "-a")),
    crossPaths := false
  )

def initSourceDirs(project: Project) : Project = {
  val sourceMainDirs = Seq(
    "src/main/java",
    "src/main/scala",
    "src/main/resources"
  )
  val sourceTestDirs = Seq(
    "src/test/java",
    "src/test/scala",
    "src/test/resources"
  )
  (sourceMainDirs ++ sourceTestDirs).map(new File(project.base, _)).foreach(_.mkdirs)
  project
}

lazy val runtimeDependencies = Seq(
  // "org.springframework.boot" % "spring-boot" % "1.4.0.RELEASE",
  // "org.springframework.boot" % "spring-boot-autoconfigure" % "1.4.0.RELEASE",
  "org.springframework.boot" % "spring-boot-starter-web" % "1.4.0.RELEASE",
  "org.springframework.boot" % "spring-boot-starter-batch" % "1.4.0.RELEASE",
  "org.springframework.boot" % "spring-boot-starter-data-jpa" % "1.4.0.RELEASE",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.6.1",

  "org.apache.commons" % "commons-lang3" % "3.4",
  "com.h2database" % "h2" % "1.4.192",
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "ch.qos.logback" % "logback-classic" % "1.1.7" % Runtime
)

lazy val testDependencies = Seq(
  "junit" % "junit" % "4.12" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.scalatest" % "scalatest_2.11" % "3.0.0" % Test
)

lazy val root = (project in file(""))
  .configure(initProject)
  .settings(
    version := "1.0.0",
    scalaVersion := "2.11.8",
    libraryDependencies ++= (runtimeDependencies ++ testDependencies)
  )
