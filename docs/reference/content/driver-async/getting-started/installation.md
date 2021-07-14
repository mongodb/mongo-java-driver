+++
date = "2015-03-17T15:36:56Z"
title = "Installation"
[menu.main]
  parent = "MongoDB Async Driver"
  identifier = "Async Installation"
  weight = 1
  pre = "<i class='fa'></i>"
+++

# Installation


The recommended way to get started using one of the drivers in your project is with a dependency management system.

{{% note class="important" %}}
When TLS/SSL is disabled, the MongoDB Async Driver requires either [Netty](http://netty.io/) or Java 7+.
When TLS/SSL is enabled, the MongoDB Async Driver requires either [Netty](http://netty.io/) or Java 8+. 
{{% /note %}}

{{< distroPicker >}}

## MongoDB Async Driver

The MongoDB Async Driver provides asynchronous API that can leverage either Netty or Java 7's AsynchronousSocketChannel for fast and non-blocking I/O.

{{< install artifactId="mongodb-driver-async" version="3.12.9" dependencies="true">}}
