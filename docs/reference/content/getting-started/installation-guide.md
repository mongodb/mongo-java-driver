+++
date = "2015-03-17T15:36:56Z"
title = "Installation Guide"
[menu.main]
  parent = "Getting Started"
  weight = 1
  pre = "<i class='fa'></i>"
+++

# Installation

The recommended way to get started using one of the drivers in your project is with a dependency management system.

{{< distroPicker >}}

## MongoDB Java Driver

This jar that contains everything you need including the BSON library.

{{< install artifactId="mongo-java-driver" version="2.13.1" >}}

## BSON

This library comprehensively supports [BSON](http://www.bsonspec.org),
the data storage and network transfer format that MongoDB uses for "documents".
BSON is short for Binary [JSON](http://json.org/), is a binary-encoded serialization of JSON-like documents.

{{< install artifactId="bson" version="2.13.1" >}}
