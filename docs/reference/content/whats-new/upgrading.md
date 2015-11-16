+++
date = "2015-03-19T12:53:39-04:00"
title = "Upgrading to 3.2"
[menu.main]
  parent = "Whats New"
  identifier = "Upgrading to 3.2"
  weight = 40
  pre = "<i class='fa fa-wrench'></i>"
+++

# Upgrading from 3.1.x

The 3.2 release is binary and source compatible with the 3.1 release, except for methods that have been added to interfaces that have 
been marked as unstable.
 
# Upgrading from 2.x

Please see the Upgrading guide in the 3.0 driver reference documentation.

## System Requirements

The minimum JVM is now Java 6: however, specific features require Java 7:

- SSL support requires Java 7 in order to perform host name verification, which is enabled by default.  See below and on
[SSL]({{< relref "driver/reference/connecting/ssl.md" >}}) for details on how to disable host name verification.
- The asynchronous API requires Java 7, as by default it relies on
[`AsynchronousSocketChannel`](http://docs.oracle.com/javase/7/docs/api/java/nio/channels/AsynchronousSocketChannel.html) for
its implementation.  See [Async]({{< ref "driver-async/index.md" >}}) for details on configuring the driver to use [Netty](http://netty.io/) instead.
