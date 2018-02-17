+++
date = "2017-06-16T22:05:03-04:00"
title = "Compression"
[menu.main]
  parent = "Connect to MongoDB"
  identifier = "Compression"
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

### Specify compression via `MongoClientURI`

```java
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
```

To specify compression with [`MongoClientURI`]({{<apiref "com/mongodb/MongoClientURI">}}), specify `compressors` as part of the connection
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
ConnectionString connectionString = new ConnectionString("mongodb://localhost/?compressors=snappy,zlib");
MongoClient mongoClient = MongoClients.create(connectionString);
```

to configure multiple compressors. 

In all cases the driver will use the first compressor in the list for which the server advertises support. 

### Specify compression via `MongoClientOptions`

```java
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCompressor;
import java.util.Arrays;
```

To specify compression with [`MongoClientSettings`]({{<apiref "com/mongodb/MongoClientSettings">}}), set the `compressors` property 
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

 