+++
date = "2015-03-19T16:39:22-04:00"
draft = true
title = "System Requirements"
[menu.main]
  weight = 20
  parent = "Reference"
  pre = "<i class='fa'></i>"
+++

# System Requirements

The 3.0 Java driver will run with Java 6 or later.  However, specific features require Java 7:

- By default SSL support requires Java 7 in order to perform SSL host name verification, which is enabled by default.  See
[SSL]({{< relref "connecting.md#SSL" >}}) for details on how to disable host name verification.
- By default the asynchronous API requires Java 7, as it relies on
[AsynchronousSocketChannel](http://docs.oracle.com/javase/7/docs/api/java/nio/channels/AsynchronousSocketChannel.html) for
its implementation.  See [Async]({{< ref "async" >}}) for details on how to use [Netty](http://netty.io/) instead.
