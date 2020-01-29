+++
date = "2017-06-16T22:05:03-04:00"
title = "Compression"
[menu.main]
  parent = "Scala Connect to MongoDB"
  identifier = "Scala Compression"
  weight = 25
  pre = "<i class='fa'></i>"
+++

## Compression

The Java driver supports compression of messages to and from MongoDB servers.  The driver implements the three algorithms that are 
supported by MongoDB servers:

* [Snappy](https://google.github.io/snappy/): Snappy compression can be used when connecting to MongoDB servers starting with the 3.4 
release.
* [Zlib](https://zlib.net/): Zlib compression can be used when connecting to MongoDB servers starting with the 3.6 release.
* [Zstandard](https://github.com/facebook/zstd/): Zstandard compression can be used when connecting to MongoDB servers starting with the 4.2 release.

The driver will negotiate which, if any, compression algorithm is used based on capabilities advertised by the server in
the [ismaster]({{<docsref "reference/command/isMaster/" >}}) command response. 

### Specify compression via `ConnectionString`

```scala
import org.mongodb.scala._
```

To specify compression with [`ConnectionString`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ConnectionString$.html" >}}), specify `compressors` as part of the connection
string, as in:

```scala
val mongoClient: MongoClient = MongoClient("mongodb://localhost/?compressors=snappy")
```

for Snappy compression, or

```scala
val mongoClient: MongoClient = MongoClient("mongodb://localhost/?compressors=zlib")
```

for zlib compression, or 

```scala
val mongoClient: MongoClient = MongoClient("mongodb://localhost/?compressors=zstd")
```

for Zstandard compression, or 

```scala
val mongoClient: MongoClient = MongoClient("mongodb://localhost/?compressors=snappy,zlib,zstd")
```

to configure multiple compressors. 

In all cases the driver will use the first compressor in the list for which the server advertises support. 

### Specify compression via `MongoClientSettings`

```scala
import org.mongodb.scala._

import scala.collection.JavaConverters._
```

To specify compression with [`MongoClientSettings`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoClientSettings$.html" >}}), set the `compressors` property 
to a list of `MongoCompressor` instances:

```scala
val settings = MongoClientSettings.builder()
  .compressorList(List(MongoCompressor.createSnappyCompressor).asJava)
  .build()
val client = MongoClient(settings)
```

for Snappy compression, or

```scala
val settings = MongoClientSettings.builder()
  .compressorList(List(MongoCompressor.createZlibCompressor).asJava)
  .build()
val client = MongoClient(settings)
```

for zlib compression, or

```scala
val settings = MongoClientSettings.builder()
  .compressorList(List(MongoCompressor.createZstdCompressor).asJava)
  .build()
val client = MongoClient(settings)
```

for Zstandard compression, or

```scala
val settings = MongoClientSettings.builder()
  .compressorList(List(MongoCompressor.createSnappyCompressor,
                       MongoCompressor.createZlibCompressor,
                       MongoCompressor.createZstdCompressor).asJava)
  .build()
val client = MongoClient(settings)
```

to configure multiple compressors. 

As with configuration with a URI, the driver will use the first compressor in the list for which the server advertises support. 

### Dependencies

As the JDK has no built-in support for Snappy or Zstandard, the driver takes a dependency on existing open-source Snappy and Zstandard implementations.  See the
[snappy-java Github repository](https://github.com/xerial/snappy-java) and the
[zstd-java Github repository](https://github.com/luben/zstd-jni) for details.
 