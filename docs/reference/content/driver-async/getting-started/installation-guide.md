+++
date = "2015-03-17T15:36:56Z"
title = "Installation Guide"
[menu.main]
  parent = "Async Getting Started"
  identifier = "Async Installation Guide"
  weight = 1
  pre = "<i class='fa'></i>"
+++

# Installation


The recommended way to get started using one of the drivers in your project is with a dependency management system.

{{% note class="important" %}}
The MongoDB Async Driver requires either [Netty](http://netty.io/) or Java 7.
{{% /note %}}

{{< distroPicker >}}

## MongoDB Async Driver
The new asynchronous API that can leverage either Netty or Java 7's AsynchronousSocketChannel for fast and non-blocking IO.

{{< install artifactId="mongodb-driver-async" version="3.2.0" dependencies="true">}}
