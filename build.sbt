scalafmtOnCompile in Compile := true

organization := "com.filipkr"
name := "spark-utils"
version := "0.1.0"

scalaVersion := "2.12.15"
val sparkVersion = "3.4.1"

developers := List(
  Developer(
    "filipkr",
    "Filip Kristofiak",
    "filip.kristofiak@gmail.com",
    url("https://github.com/filipkristofiak")
  )
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
)
