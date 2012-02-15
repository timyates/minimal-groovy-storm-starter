This is a minimal project setup to work with the [Storm distributed system](https://github.com/nathanmarz/storm) with [Groovy](https://github.com/groovy/groovy-core) using [Gradle](https://github.com/gradle/gradle/) as a build tool.

It is basically a copy of the word-count example in the [Storm starter](https://github.com/nathanmarz/storm-starter) project.

To get started, simply [download the latest version](http://www.gradle.org/downloads) of Gradle, cd to the location of this project and type

    gradle run

All dependencies will be fetched, and the wordcount example will run.

This has only so-far been confirmed as working on a local topology, I haven't yet tried it on a cluster.