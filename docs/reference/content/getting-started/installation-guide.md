+++
date = "2015-03-17T15:36:56Z"
title = "Installation Guide"
[menu.main]
  parent = "Getting Started"
  weight = 1
  pre = "<i class='fa'></i>"
+++

# Installation

There are three different MongoDB drivers available in the 3.0 release and a standalone BSON library.
The recommended way to get started using one of the drivers in your project is with a dependency management system.

{{< distroPicker >}}

## MongoDB Driver  

The MongoDB Driver is the updated synchronous Java driver that includes the
legacy API as well as a new generic MongoCollection interface that complies with
a new cross-driver CRUD specification.

{{< install artifactId="mongodb-driver" version="3.0.0-rc1" >}}


## MongoDB Async Driver
A new asynchronous API that can leverage either Netty or Java 7's AsynchronousSocketChannel for fast and non-blocking IO.

{{< install artifactId="mongodb-driver" version="3.0.0-rc1" >}}

## MongoDB Core Driver
The MongoDB Driver and Async Driver are both built on top of this new core library. Anyone can use it to build alternative or experimental high-level APIs.

{{< install artifactId="mongodb-driver-core" version="3.0.0-rc1" >}}

## BSON

This library comprehensively supports [BSON](http://www.bsonspec.org),
the data storage and network transfer format that MongoDB uses for "documents".
BSON is short for Binary [JSON](http://json.org/), is a binary-encoded serialization of JSON-like documents.

{{< install artifactId="bson" version="3.0.0-rc1" >}}

## Uber MongoDB Java Driver
This is the legacy uber jar that contains everything you need; the BSON library, the core library and the mongodb-driver.

{{< install artifactId="mongo-java-driver" version="3.0.0-rc1" >}}
