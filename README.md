## Release Notes

Release notes are available [here](https://github.com/mongodb/mongo-java-driver/releases).

## Documentation

Reference and API documentation is available [here](http://mongodb.github.io/mongo-java-driver/).

## Support / Feedback

For issues with, questions about, or feedback for the MongoDB Java driver, please look into
our [support channels](https://www.mongodb.com/docs/manual/support/). Please
do not email any of the Java driver developers directly with issues or
questions - you're more likely to get an answer on the [MongoDB Community Forums](https://community.mongodb.com/tags/c/drivers-odms-connectors/7/java-driver).

At a minimum, please include in your description the exact version of the driver that you are using.  If you are having
connectivity issues, it's often also useful to paste in the line of code where you construct the MongoClient instance,
along with the values of all parameters that you pass to the constructor. You should also check your application logs for
any connectivity-related exceptions and post those as well.

## Bugs / Feature Requests

Think you’ve found a bug? Want to see a new feature in the Java driver? Please open a
case in our issue management tool, JIRA:

- [Create an account and login](https://jira.mongodb.org).
- Navigate to [the JAVA project](https://jira.mongodb.org/browse/JAVA).
- Click **Create Issue** - Please provide as much information as possible about the issue type and how to reproduce it.

Bug reports in JIRA for the driver and the Core Server (i.e. SERVER) project are **public**.

If you’ve identified a security vulnerability in a driver or any other
MongoDB project, please report it according to the [instructions here](https://www.mongodb.com/docs/manual/tutorial/create-a-vulnerability-report).

## Versioning

Major increments (such as 2.x -> 3.x) will occur when break changes are being made to the public API.  All methods and
classes removed in a major release will have been deprecated in a prior release of the previous major release branch, and/or otherwise
called out in the release notes.

Minor 3.x increments (such as 3.1, 3.2, etc) will occur when non-trivial new functionality is added or significant enhancements or bug
fixes occur that may have behavioral changes that may affect some edge cases (such as dependence on behavior resulting from a bug). An
example of an enhancement is a method or class added to support new functionality added to the MongoDB server.   Minor releases will
almost always be binary compatible with prior minor releases from the same major release branch, except as noted below.

Patch 3.x.y increments (such as 3.0.0 -> 3.0.1, 3.1.1 -> 3.1.2, etc) will occur for bug fixes only and will always be binary compatible
with prior patch releases of the same minor release branch.

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
$ git clone https://github.com/mongodb/mongo-java-driver.git
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

## Maintainers

* Jeff Yemin           jeff.yemin@mongodb.com
* Ross Lawley          ross@mongodb.com
* John Stewart         john.stewart@mongodb.com

Additional contributors can be found [here](https://github.com/mongodb/mongo-java-driver/graphs/contributors).

## Supporters

JetBrains is supporting this open source project with:

[![Intellij IDEA](http://www.jetbrains.com/img/logos/logo_intellij_idea.png)](http://www.jetbrains.com/idea/)

