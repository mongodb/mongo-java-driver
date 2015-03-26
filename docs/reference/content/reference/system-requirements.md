+++
date = "2015-03-19T16:39:22-04:00"
title = "System Requirements"
[menu.main]
  weight = 20
  parent = "Reference"
  pre = "<i class='fa'></i>"
+++

# System Requirements

The 3.0 Java driver will run with Java 6 or later: however, specific features require Java 7:

- SSL support requires Java 7 in order to perform host name verification, which is enabled by default.  See
[SSL]({{< relref "connecting.md#SSL" >}}) for details on how to disable host name verification.
- The asynchronous API requires Java 7, as by default it relies on
[AsynchronousSocketChannel](http://docs.oracle.com/javase/7/docs/api/java/nio/channels/AsynchronousSocketChannel.html) for
its implementation.  See [Async]({{< ref "async" >}}) for details on configuring the driver to use [Netty](http://netty.io/) instead.
