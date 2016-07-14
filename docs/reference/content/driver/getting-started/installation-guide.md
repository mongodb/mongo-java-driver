+++
date = "2015-03-17T15:36:56Z"
title = "Installation Guide"
[menu.main]
  parent = "Sync Getting Started"
  identifier = "Sync Installation Guide"
  weight = 1
  pre = "<i class='fa'></i>"
+++

# Installation

There are two Maven artifacts available in the 3.3 release. The preferred artifact for new applications is `mongodb-driver`;
however, we still publish the legacy `mongo-java-driver` uber-jar.
The recommended way to get started using one of the drivers in your project is with a dependency management system.

{{< distroPicker >}}

## MongoDB Driver  

The MongoDB Driver is the updated synchronous Java driver that includes the
legacy API as well as a new generic MongoCollection interface that complies with
a new cross-driver CRUD specification.

{{% note class="important" %}}
For OSGi-based applications: due to the fact that there are classes from the `com.mongodb` package in both this artifact and in the 
`mongodb-driver-core` artifact, on which this depends, this artifact is *not* an OSGi bundle.  Please use the `mongo-java-driver` uber 
jar (described below) instead. 
{{% /note %}}

{{< install artifactId="mongodb-driver" version="3.3.0" dependencies="true">}}


## Uber MongoDB Java Driver
An uber jar that contains everything you need; the BSON library, the core library and the mongodb-driver.

{{% note %}}
For OSGi-based applications: this artifact is a valid OSGi bundle. 
{{% /note %}}

{{< install artifactId="mongo-java-driver" version="3.3.0">}}
