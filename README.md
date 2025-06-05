## Release Notes

Release notes are available [here](https://github.com/mongodb/mongo-java-driver/releases).

## Documentation

Reference and API documentation for the Java driver is available [here](https://www.mongodb.com/docs/drivers/java/sync/current/). 

Reference and API documentation for the Kotlin driver is available [here](https://www.mongodb.com/docs/drivers/kotlin/coroutine/current/).

Reference and API documentation for the Scala driver is available [here](https://www.mongodb.com/docs/languages/scala/scala-driver/current/). 

## Tutorials / Training

For tutorials on how to use the MongoDB JVM Drivers, please reference [MongoDB University](https://learn.mongodb.com/). Additional tutorials, videos, and code examples using both the Java Driver and the Kotlin Driver can also be found in the [MongoDB Developer Center](https://www.mongodb.com/developer/).

## Support / Feedback

For issues with, questions about, or feedback for the MongoDB Java, Kotlin, and Scala drivers, please look into
our [support channels](https://www.mongodb.com/docs/manual/support/). Please
do not email any of the driver developers directly with issues or
questions - you're more likely to get an answer on the [MongoDB Community Forums](https://community.mongodb.com/tags/c/drivers-odms-connectors/7/java-driver) or [StackOverflow](https://stackoverflow.com/questions/tagged/mongodb+java).

At a minimum, please include in your description the exact version of the driver that you are using.  If you are having
connectivity issues, it's often also useful to paste in the line of code where you construct the MongoClient instance,
along with the values of all parameters that you pass to the constructor. You should also check your application logs for
any connectivity-related exceptions and post those as well.

## Bugs / Feature Requests

Think you’ve found a bug in the Java, Kotlin, or Scala drivers? Want to see a new feature in the drivers? Please open a
case in our issue management tool, JIRA:

- [Create an account and login](https://jira.mongodb.org).
- Navigate to [the JAVA project](https://jira.mongodb.org/browse/JAVA).
- Click **Create Issue** - Please provide as much information as possible about the issue type, which driver you are using, and how to reproduce your issue.

Bug reports in JIRA for the driver and the Core Server (i.e. SERVER) project are **public**.

If you’ve identified a security vulnerability in a driver or any other
MongoDB project, please report it according to the [instructions here](https://www.mongodb.com/docs/manual/tutorial/create-a-vulnerability-report).

## Versioning

We generally follow [semantic versioning](https://semver.org/spec/v2.0.0.html) when releasing.

#### @Alpha

APIs marked with the `@Alpha` annotation are in the early stages of development, subject to incompatible changes, 
or even removal, in a future release and may lack some intended features. An APIs bearing `@Alpha` annotation may 
contain known issues affecting functionality, performance, and stability. They are also exempt from any compatibility 
guarantees made by its containing library.

It is inadvisable for <i>applications</i> to use Alpha APIs in production environments or for <i>libraries</i>
(which get included on users' CLASSPATHs, outside the library developers' control) to depend on these APIs. Alpha APIs
are intended for <b>experimental purposes</b> only.

#### @Beta

APIs marked with the `@Beta` annotation at the class or method level are subject to change. They can be modified in any way, or even
removed, at any time. If your code is a library itself (i.e. it is used on the CLASSPATH of users outside your own control), you should not
use beta APIs, unless you repackage them (e.g. by using shading, etc).

#### @Deprecated

APIs marked with the `@Deprecated` annotation at the class or method level will remain supported until the next major release but it is
recommended to stop using them.

#### com.mongodb.internal.*

All code inside the `com.mongodb.internal.*` packages is considered private API and should not be relied upon at all. It can change at any
time.

## Binaries

Binaries and dependency information for Maven, Gradle, Ivy and others can be found at
[http://search.maven.org](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.mongodb%22%20AND%20a%3A%22mongodb-driver-sync%22).

Example for Maven:

```xml
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-sync</artifactId>
    <version>x.y.z</version>
</dependency>
```
Snapshot builds are also published regulary via Sonatype.

Example for Maven:

```xml
    <repositories>
        <repository>
            <id>sonatype-snapshot</id>
            <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
        </repository>
    </repositories>
```

## Build

Java 17+ and git is required to build and compile the source. To build and test the driver:

```
$ git clone --recurse-submodules https://github.com/mongodb/mongo-java-driver.git
$ cd mongo-java-driver
$ ./gradlew check
```

The test suite requires mongod to be running with [`enableTestCommands`](https://www.mongodb.com/docs/manual/reference/parameters/#param.enableTestCommands), which may be set with the `--setParameter enableTestCommands=1`
command-line parameter:
```
$ mkdir -p data/db
$ mongod --dbpath ./data/db --logpath ./data/mongod.log --port 27017 --logappend --fork --setParameter enableTestCommands=1
```

If you encounter `"Too many open files"` errors when running the tests then you will need to increase 
the number of available file descriptors prior to starting mongod as described in [https://www.mongodb.com/docs/manual/reference/ulimit/](https://www.mongodb.com/docs/manual/reference/ulimit/)

## IntelliJ IDEA

A couple of manual configuration steps are required to run the code in IntelliJ:

- Java 17+ is required to build and compile the source.

- **Error:** `java: cannot find symbol: class SNIHostName location: package javax.net.ssl`<br>
 **Fix:** Settings/Preferences > Build, Execution, Deployment > Compiler > Java Compiler - untick "Use '--release' option for 
  cross-compilation (Java 9 and later)"

- **Error:** `java: package com.mongodb.internal.build does not exist`<br>
 **Fixes:** Any of the following: <br>
  - Run the `generateBuildConfig` task: eg: `./gradlew generateBuildConfig` or via Gradle > driver-core > Tasks > buildconfig >
 generateBuildConfig
  - Set `generateBuildConfig` to execute Before Build. via Gradle > Tasks > buildconfig > right click generateBuildConfig - click on 
   "Execute Before Build" 
  - Delegate all build actions to Gradle: Settings/Preferences > Build, Execution, Deployment > Build Tools > Gradle > Build and run 
  using/Run tests using - select "Gradle"
