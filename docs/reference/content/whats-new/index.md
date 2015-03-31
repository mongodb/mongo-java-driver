+++
date = "2015-03-19T12:53:35-04:00"
title = "What's New"
[menu.main]
  identifier = "Whats New"
  weight = 10
  pre = "<i class='fa fa-cog'></i>"
+++

# What's New in 3.0

Key features of the 3.0 driver include:

- A generic [`MongoCollection`]({{< apiref "com/mongodb/client/MongoCollection" >}}) interface that complies with a new cross-driver 
[CRUD specification](https://github.com/mongodb/specifications/blob/master/source/crud/crud.rst).
- A new [asynchronous API](https://github.com/mongodb/mongo-java-driver/tree/master/driver-async) that can leverage either 
[Netty](http://netty.io/) or Java 7's 
[AsynchronousSocketChannel](http://docs.oracle .com/javase/7/docs/api/java/nio/channels/AsynchronousSocketChannel.html)
- A new [Codec]({{< ref "bson/codecs.md" >}}) infrastructure that you can use to build high-performance
 encoders and decoders without requiring an intermediate Map instance.
- A new core driver on top of which you can build alternative or experimental driver APIs

## Upgrading

See the [upgrading guide]({{<ref "whats-new/upgrading.md">}}) on how to upgrade to 3.0
