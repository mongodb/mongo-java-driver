+++
date = "2017-06-16T22:05:03-04:00"
title = "Compression"
[menu.main]
  parent = "Async Connection Settings"
  identifier = "Async Compression"
  weight = 25
  pre = "<i class='fa'></i>"
+++

## Compression

The Java driver supports compression of messages to and from MongoDB servers.  The driver implements the two algorithms that are 
supported by MongoDB servers:

* [Snappy](https://google.github.io/snappy/): Snappy compression can be used when connecting to MongoDB servers starting with the 3.4 
release.
* [Zlib](https://zlib.net/): Zlib compression can be used when connecting to MongoDB servers starting with the 3.6 release.

The driver will negotiate which, if any, compression algorithm is used based on capabilities advertised by the server in
the [ismaster]({{<docsref "reference/command/isMaster/">}}) command response. 

### Specify compression via connection string

```java
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoClient;

```

To specify compression with a connection string, specify `compressors` as part of the connection string, as in:

```java
MongoClient mongoClient = MongoClients.create("mongodb://localhost/?compressors=snappy");
```

for Snappy compression, or

```java
MongoClient mongoClient = MongoClients.create("mongodb://localhost/?compressors=zlib");
```

for zlib compression, or 


```java
MongoClient mongoClient = MongoClients.create("mongodb://localhost/?compressors=snappy,zlib");
```

to configure multiple compressors. 

In all cases the driver will use the first compressor in the list for which the server advertises support. 

### Specify compression via `MongoClientSettings`

```java
import com.mongodb.connection.ClusterSettings;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoClient;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCompressor;
import java.util.Arrays;

```

To specify compression with [`MongoClientSettings`]({{<apiref "com/mongodb/MongoClientSettings">}}), set the `compressors` property
to a list of `MongoCompressor` instances:

```java
ClusterSettings clusterSettings = ClusterSettings.builder().hosts(Arrays.asList(new ServerAddress("localhost"))).build();
MongoClientSettings settings = MongoClientSettings.builder()
                                                  .clusterSettings(clusterSettings);
                                                  .compressorList(Arrays.asList(MongoCompressor.createSnappyCompressor()))
                                                  .build();
MongoClient client = MongoClients.create(settings);
```

for Snappy compression, or

```java
ClusterSettings clusterSettings = ClusterSettings.builder().hosts(Arrays.asList(new ServerAddress("localhost"))).build();
MongoClientSettings settings = MongoClientSettings.builder()
                                                  .clusterSettings(clusterSettings);
                                                  .compressorList(Arrays.asList(MongoCompressor.createZlibCompressor()))
                                                  .build();
MongoClient client = MongoClients.create(settings);
```

for zlib compression, or

```java
ClusterSettings clusterSettings = ClusterSettings.builder().hosts(Arrays.asList(new ServerAddress("localhost"))).build();
MongoClientSettings settings = MongoClientSettings.builder()
                                                  .clusterSettings(clusterSettings);
                                                  .compressorList(Arrays.asList(MongoCompressor.createSnappyCompressor(),
                                                                                MongoCompressor.createZlibCompressor()))
                                                  .build();
MongoClient client = MongoClients.create(settings);
```

to configure multiple compressors. 

As with configuration with a URI, the driver will use the first compressor in the list for which the server advertises support. 

### Dependencies

As the JDK has no built-in support for Snappy, the driver takes a dependency on an existing open-source Snappy implementation.  See the
[snappy-java Github repository](https://github.com/xerial/snappy-java) for details.

