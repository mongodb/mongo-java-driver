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

There are two Maven artifacts available in the 3.4 release. The preferred artifact for new applications is `mongodb-driver`
however, we still publish the legacy `mongo-java-driver` uber-jar.

{{< distroPicker >}}

## MongoDB Driver  

The MongoDB Driver ``mongodb-driver`` is the updated synchronous Java driver that includes the legacy API as well as a new generic `MongoCollection` interface that complies with a new cross-driver CRUD specification.

{{% note class="important" %}}
`mongodb-driver` is *not* an OSGi bundle: both `mongodb-driver` and `mongodb-driver-core`, a dependency of `mongodb-driver`, include classes from the `com.mongodb` package.

For OSGi-based applications, use the [mongo-java-driver](#uber-jar-legacy) uber jar instead.

{{% /note %}}

{{< install artifactId="mongodb-driver" version="3.4.1" dependencies="true">}}


## Uber Jar (Legacy)

For new applications, the preferred artifact is [mongodb-driver](#mongodb-driver); however, the legacy `mongo-java-driver` uber jar is still available.  The uber jar contains: the BSON library, the core library, and the `mongodb-driver`.


{{% note %}}
The `mongo-java-driver` artifact is a valid OSGi bundle.
{{% /note %}}

{{< install artifactId="mongo-java-driver" version="3.4.1">}}
