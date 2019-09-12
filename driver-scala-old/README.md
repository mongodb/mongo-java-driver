## MongoDB Scala Driver ##

A modern idiomatic MongoDB Scala Driver.

[![Build Status](https://travis-ci.org/mongodb/mongo-scala-driver.svg?branch=master)](https://travis-ci.org/mongodb/mongo-scala-driver)

## Documentation

Documentation for all the main versions is available on http://mongodb.github.io/mongo-scala-driver

## Support / Feedback

For issues with, questions about, or feedback for the MongoDB Scala driver, please look into
our [support channels](http://www.mongodb.org/about/support). Please
do not email any of the Scala driver developers directly with issues or
questions - you're more likely to get an answer on the [mongodb-user](http://groups.google.com/group/mongodb-user) list on Google Groups.

At a minimum, please include in your description the exact version of the driver that you are using.  If you are having
connectivity issues, it's often also useful to paste in the line of code where you construct the MongoClient instance,
along with the values of all parameters that you pass to the constructor. You should also check your application logs for
any connectivity-related exceptions and post those as well.

## Bugs / Feature Requests

Think you’ve found a bug? Want to see a new feature in the Scala driver? Please open a
case in our issue management tool, JIRA:

- [Create an account and login](https://jira.mongodb.org).
- Navigate to [the SCALA project](https://jira.mongodb.org/browse/SCALA).
- Click **Create Issue** - Please provide as much information as possible about the issue type and how to reproduce it.

Bug reports in JIRA for the driver and the Core Server (i.e. SERVER) project are **public**.

If you’ve identified a security vulnerability in a driver or any other
MongoDB project, please report it according to the [instructions here](http://docs.mongodb.org/manual/tutorial/create-a-vulnerability-report).

## Versioning

Major increments (such as 1.x -> 2.x) will occur when break changes are being made to the public API.  All methods and
classes removed in a major release will have been deprecated in a prior release of the previous major release branch, and/or otherwise
called out in the release notes.

Minor 1.x increments (such as 1.1, 1.2, etc) will occur when non-trivial new functionality is added or significant enhancements or bug
fixes occur that may have behavioral changes that may affect some edge cases (such as dependence on behavior resulting from a bug). An
example of an enhancement is a method or class added to support new functionality added to the MongoDB server.   Minor releases will
almost always be binary compatible with prior minor releases from the same major release branch, exept as noted below.

Patch 1.x.y increments (such as 1.0.0 -> 1.0.1, 1.1.1 -> 1.1.2, etc) will occur for bug fixes only and will always be binary compatible
with prior patch releases of the same minor release branch.

## Binaries

Binaries and dependency information for Maven, SBT, Ivy and others can be found at
[http://search.maven.org](http://search.maven.org/#search%7Cga%7C1%7Cg%3Aorg.mongodb.scala).

## Build

To build the driver:

```
$ git clone https://github.com/mongodb/mongo-scala-driver.git
$ cd mongo-scala-driver
$ ./sbt check
```

To publish the signed jars:

```
$ ./sbt +publish-signed
```

## Maintainers

* Ross Lawley          ross@mongodb.com

Additional contributors can be found [here](https://github.com/mongodb/mongo-scala-driver/graphs/contributors).

## Supporters

JetBrains is supporting this open source project with:

[![Intellij IDEA](http://www.jetbrains.com/img/logos/logo_intellij_idea.png)](http://www.jetbrains.com/idea/)
