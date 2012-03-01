This is a minimal project setup to work with the [Storm distributed system](https://github.com/nathanmarz/storm) with [Groovy](https://github.com/groovy/groovy-core) using [Gradle](https://github.com/gradle/gradle/) as a build tool.

It is basically a copy of the word-count example in the [Storm starter](https://github.com/nathanmarz/storm-starter) project.

To get started, simply [download the latest version](http://www.gradle.org/downloads) of Gradle, cd to the location of this project and type

    gradle

All dependencies will be fetched, and the wordcount example will run.

To list all current examples, type:

    gradle tasks

And the examples will be listed under the heading `In-process Storm Example tasks`

I am looking into some examples with a working cluster, but this requires me to set up a working cluster first ;-)
