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
however, we still publish the legacy `mongodb-driver-legacy` jar.

{{< distroPicker >}}

## MongoDB Driver Sync 

The MongoDB Driver `mongodb-driver-sync` is the synchronous Java driver containing only the generic `MongoCollection` interface that 
complies with a new cross-driver CRUD specification.  It does *not* include the legacy API (e.g. `DBCollection`).

{{% note class="important" %}}

This is a Java 9-compliant module with an Automatic-Module-Name of `org.mongodb.driver.sync.client`.

The `mongodb-driver-sync` artifact is a valid OSGi bundle whose symbolic name is `org.mongodb.driver-sync`.

{{% /note %}}

{{< install artifactId="mongodb-driver-sync" version="4.3.2" dependencies="true">}}

## MongoDB Driver Legacy 

The MongoDB Legacy driver `mongodb-driver-legacy` is the legacy synchronous Java driver whose entry point is `com.mongodb.MongoClient` 
and central classes include `com.mongodb.DB`, `com.mongodb.DBCollection`, and `com.mongodb.DBCursor`.

{{% note class="important" %}}

While not deprecated, we recommend that new applications depend on the `mongodb-driver-sync` module.

{{% /note %}}

{{< install artifactId="mongodb-driver-legacy" version="4.3.2" dependencies="true">}}
