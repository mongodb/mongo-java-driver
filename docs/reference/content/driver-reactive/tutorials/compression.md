+++
date = "2017-06-16T22:05:03-04:00"
title = "Compression"
[menu.main]
  parent = "Reactive Connect to MongoDB"
  identifier = "Reactive Compression"
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

```java
import com.mongodb.ConnectionString;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoClient;
```

To specify compression with [`ConnectionString`]({{< apiref "mongodb-driver-core" "com/mongodb/ConnectionString" >}}), specify `compressors` as part of the connection
string, as in:

```java
ConnectionString connectionString = new ConnectionString("mongodb://localhost/?compressors=snappy");
MongoClient mongoClient = MongoClients.create(connectionString);
```

for Snappy compression, or

```java
ConnectionString connectionString = new ConnectionString("mongodb://localhost/?compressors=zlib");
MongoClient mongoClient = MongoClients.create(connectionString);
```

for zlib compression, or 

```java
ConnectionString connectionString = new ConnectionString("mongodb://localhost/?compressors=zstd");
MongoClient mongoClient = MongoClients.create(connectionString);
```

for Zstandard compression, or 

```java
ConnectionString connectionString = new ConnectionString("mongodb://localhost/?compressors=snappy,zlib,zstd");
MongoClient mongoClient = MongoClients.create(connectionString);
```

to configure multiple compressors. 

In all cases the driver will use the first compressor in the list for which the server advertises support. 

### Specify compression via `MongoClientSettings`

```java
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCompressor;
import java.util.Arrays;
```

To specify compression with [`MongoClientSettings`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientSettings" >}}), set the `compressors` property 
to a list of `MongoCompressor` instances:

```java
MongoClientSettings settings = MongoClientSettings.builder()
        .compressorList(Arrays.asList(MongoCompressor.createSnappyCompressor()))
        .build();
MongoClient client = MongoClients.create(settings);
```

for Snappy compression, or

```java
MongoClientSettings settings = MongoClientSettings.builder()
        .compressorList(Arrays.asList(MongoCompressor.createZlibCompressor()))
        .build();
MongoClient client = MongoClients.create(settings);
```

for zlib compression, or

```java
MongoClientSettings settings = MongoClientSettings.builder()
        .compressorList(Arrays.asList(MongoCompressor.createZstdCompressor()))
        .build();
MongoClient client = MongoClients.create(settings);
```

for Zstandard compression, or

```java
MongoClientSettings settings = MongoClientSettings.builder()
        .compressorList(Arrays.asList(MongoCompressor.createSnappyCompressor(),
                                      MongoCompressor.createZlibCompressor(),
                                      MongoCompressor.createZstdCompressor()))
        .build();
MongoClient client = MongoClients.create(settings);
```

to configure multiple compressors. 

As with configuration with a URI, the driver will use the first compressor in the list for which the server advertises support. 

### Dependencies

As the JDK has no built-in support for Snappy or Zstandard, the driver takes a dependency on existing open-source Snappy and Zstandard implementations.  See the
[snappy-java Github repository](https://github.com/xerial/snappy-java) and the
[zstd-java Github repository](https://github.com/luben/zstd-jni) for details.
 