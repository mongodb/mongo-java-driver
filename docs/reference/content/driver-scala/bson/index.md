+++
date = "2015-03-19T12:53:30-04:00"
title = "BSON"
[menu.main]
  identifier = "Scala BSON"
  parent = "MongoDB Scala Driver"
  weight = 40
  pre = "<i class='fa fa-th'></i>"
+++

## BSON

The BSON library comprehensively supports [BSON](http://www.bsonspec.org), the data storage and network transfer format that MongoDB uses for 
“documents". BSON, short for Binary [JSON](http://json.org/), is a binary-encoded serialization of JSON-like documents.

For everyday usage there are type aliases and companion objects available from the [`org.mongodb.scala.bson`]({{< apiref="org.mongodb.scala.bson.package" >}}) package.

- [Documents]({{< relref "documents.md" >}}): Documentation of the driver's support for BSON document representations
- [Extended JSON]({{< relref "extended-json.md" >}}): Documentation of the driver's support for MongoDB Extended JSON

For advanced usage you may need to use `org.bson` directly. See the core Java driver documentation for:

- [Readers and Writers]({{< relref "bson/readers-and-writers.md" >}}): Documentation of the driver's support for stream-based reading and writing
 of BSON documents
- [Codec and CodecRegistry]({{< relref "bson/codecs.md" >}}): Documentation of the driver's `Codec` API, an abstraction for producing and 
consuming  BSON document representations using the stream-based readers and writers
