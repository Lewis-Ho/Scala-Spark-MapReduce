## Scala-Spark-MapReduce

This is a guild for setup Spark on Intellij IDE. Example provided to demonstrate running spark.

## Motivation

There are so many ways to set things up but I found that they are confusing. Plus, setup an envirnoment on intellij always creates errors or require you to restart the IDE in order to make dependencies work on your setting. That's why I want to provide the way that how I do.

## Installation

Credit to [SETTING UP INTELLIJ FOR SPARK](http://danielnee.com/archives/307), bascially two important files that you have to create, plugins.sbt and build.sbt. You can manually create them as follow:
```
mkdir -p src/main/scala
mkdir -p src/test/scala
mkdir project
```

In project dir that you just created:
`touch plugins.sbt`
And write this to it:
`addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")`

Back to project root, do: 
`touch build.sbt`
Same thing, write this:
```
name := "Project Name"

version := "1.0"
 
scalaVersion := "2.10.4"
 
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0"
```

So actually your folder should be like this: 
```
$ find .
.
./build.sbt
./project
./project/plugins.sbt
./src/main/scala
./src/test/scala
```

On project root, run:
```
sbt update
sbt gen-idea
```

That's it!

## Tests

Will describe and show how to run the tests with code examples.

## License

Free to do anything with the content in this repo. Please put the reference to this repo if you want to share this.
