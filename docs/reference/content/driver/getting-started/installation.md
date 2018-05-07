+++
date = "2015-03-17T15:36:56Z"
title = "Installation"
[menu.main]
  parent = "MongoDB Driver"
  identifier = "Sync Installation"
  weight = 1
  pre = "<i class='fa'></i>"
+++

# Installation

The recommended way to get started using one of the drivers in your
project is with a dependency management system.

There are two Maven artifacts available in the release. The preferred artifact for new applications is `mongodb-driver-sync`
however, we still publish the legacy `mongo-java-driver` uber-jar as well as the `mongodb-driver` jar introduced in 3.0.

{{< distroPicker >}}

## MongoDB Driver Sync 

The MongoDB Driver `mongodb-driver-sync` is the synchronous Java driver containing only the generic `MongoCollection` interface that 
complies with a new cross-driver CRUD specification.  It does *not* include the legacy API (e.g. `DBCollection`).

{{% note class="important" %}}

This is a Java 9-compliant module with an Automatic-Module-Name of `org.mongodb.driver.sync.client`.

The `mongodb-driver-sync` artifact is a valid OSGi bundle whose symbolic name is `org.mongodb.driver-sync`.

{{% /note %}}

{{< install artifactId="mongodb-driver-sync" version="3.8.0-beta2" dependencies="true">}}

## MongoDB Driver  

The MongoDB Driver `mongodb-driver` is the updated synchronous Java driver that includes the legacy API as well as a new generic `MongoCollection` interface that complies with a new cross-driver CRUD specification.

{{% note class="important" %}}
`mongodb-driver` is *not* an OSGi bundle: both `mongodb-driver` and `mongodb-driver-core`, a dependency of `mongodb-driver`, include classes from the `com.mongodb` package.

For OSGi-based applications, use the [mongodb-driver-sync](#mongodb-driver-sync) or the [mongo-java-driver](#uber-jar-legacy) uber jar instead.

It is also *not* a Java 9 module.

{{% /note %}}

{{< install artifactId="mongodb-driver" version="3.8.0-beta2" dependencies="true">}}


## Uber Jar (Legacy)

For new applications, the preferred artifact is [mongodb-driver-sync](#mongodb-driver-sync); however, the legacy `mongo-java-driver` uber
jar is still available.  The uber jar contains: the BSON library, the core library, and the `mongodb-driver`.


{{% note %}}
This is a Java 9-compliant module with an Automatic-Module-Name of `org.mongodb.driver.sync.client`.

The `mongo-java-driver` artifact is a valid OSGi bundle whose symbolic name is `org.mongodb.mongo-java-driver`.
{{% /note %}}

{{< install artifactId="mongo-java-driver" version="3.8.0-beta2">}}
