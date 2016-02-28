import sbt._

object MyBuild extends Build {

    lazy val root = Project("root", file(".")) dependsOn(eventsModelProject)
    lazy val eventsModelProject = RootProject(uri("https://gitlab.com/vitalcode/events-model.git"))

}