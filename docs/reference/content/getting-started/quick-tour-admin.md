+++
date = "2015-03-17T15:36:56Z"
title = "Admin Quick Tour"
[menu.main]
  parent = "Getting Started"
  weight = 20
  pre = "<i class='fa'></i>"
+++

# MongoDB Driver Admin Quick Tour

This is the second part of the MongoDB driver quick tour. In the
[quick tour]({{< relref "getting-started/quick-tour.md" >}}) we looked at how to
use the Java driver to execute basic CRUD operations.  In this section we'll look at some of the
administrative features available in the driver.

The following code snippets come from the `QuickTourAdmin.java` example code
that can be found with the [driver
source]({{< srcref "src/examples/example/QuickTourAdmin.java">}}).

{{% note %}}
See the [installation guide]({{< relref "getting-started/installation-guide.md" >}})
for instructions on how to install the MongoDB Driver.
{{% /note %}}

## Setup

To get use started we'll quickly connect and create a `mongoClient`, `database` and `collection`
variable for use in the examples below:

```java
MongoClient mongoClient = new MongoClient();
MongoDatabase database = mongoClient.getDatabase("mydb");
MongoCollection<Document> collection = database.getCollection("test");
```

## Getting A List of Databases

You can get a list of the available databases:

```java
MongoClient mongoClient = new MongoClient();

for (String s : mongoClient.getDatabaseNames()) {
   System.out.println(s);
}
```

Calling `mongoClient.getDB()` does not create a database. Only when a
database is written to will a database be created. Examples would be
creating an index or collection or inserting a document.

## Dropping A Database

You can drop a database by name using a `MongoClient` instance:

```java
MongoClient mongoClient = new MongoClient();
mongoClient.dropDatabase("databaseToBeDropped");
```

## Creating A Collection

There are two ways to create a collection. Inserting a document will
create the collection if it doesn't exist or calling the
[createCollection](http://docs.mongodb.org/manual/reference/method/db.createCollection)
command.

An example of creating a capped sized to 1 megabyte:

```java
db = mongoClient.getDB("mydb");
db.createCollection("testCollection", new BasicDBObject("capped", true)
        .append("size", 1048576));
```

## Getting A List of Collections

You can get a list of the available collections in a database:

```java
for (String s : db.getCollectionNames()) {
   System.out.println(s);
}
```

It should output

```ini
system.indexes
testCollection
```

{{% note %}}
The system.indexes collection is automatically created and lists all
the indexes in the database and shouldn't be accessed directly.
{{% /note %}}

## Dropping A Collection

You can drop a collection by using the drop() method:

```java
DBCollection coll = db.getCollection("testCollection");
coll.drop();
System.out.println(db.getCollectionNames());
```

And you should notice that testCollection has been dropped.

## Getting a List of Indexes on a Collection

You can get a list of the indexes on a collection:

```java
List<DBObject> list = coll.getIndexInfo();

for (DBObject o : list) {
   System.out.println(o.get("key"));
}
```

and you should see something like

```json
{ "v" : 1 , "key" : { "_id" : 1} , "name" : "_id_" , "ns" : "mydb.testCollection"}
{ "v" : 1 , "key" : { "i" : 1} , "name" : "i_1" , "ns" : "mydb.testCollection"}
{ "v" : 1 , "key" : { "loc" : "2dsphere"} , "name" : "loc_2dsphere" , ... }
{ "v" : 1 , "key" : { "_fts" : "text" , "_ftsx" : 1} , "name" : "content_text" , ... }
```

## Creating An Index

MongoDB supports indexes, and they are very easy to add on a collection.
To create an index, you just specify the field that should be indexed,
and specify if you want the index to be ascending (`1`) or descending
(`-1`). The following creates an ascending index on the `i` field :

```java
coll.createIndex(new BasicDBObject("i", 1));  // create index on "i", ascending
```

## Geo indexes

MongoDB supports various [geospatial indexes]({{< docref "core/geospatial-indexes/" >}})
in this example we'll be creating a 2dsphere index which we can query using standard
GeoJson markup. To create a 2dsphere index specify the string literal
"2dsphere" in the index document:

```java
coll.createIndex(new BasicDBObject("loc", "2dsphere"));
```

There are various ways to
query a [2dsphere index]({{< docref "/tutorial/query-a-2dsphere-index">}} this example
finds places within 500 meters of a location:

```java
BasicDBList coordinates = new BasicDBList();
coordinates.put(0, -73.97);
coordinates.put(1, 40.77);
coll.insert(new BasicDBObject("name", "Central Park")
                .append("loc", new BasicDBObject("type", "Point").append("coordinates", coordinates))
                .append("category", "Parks"));

coordinates.put(0, -73.88);
coordinates.put(1, 40.78);
coll.insert(new BasicDBObject("name", "La Guardia Airport")
        .append("loc", new BasicDBObject("type", "Point").append("coordinates", coordinates))
        .append("category", "Airport"));


// Find whats within 500m of my location
BasicDBList myLocation = new BasicDBList();
myLocation.put(0, -73.965);
myLocation.put(1, 40.769);
myDoc = coll.findOne(
            new BasicDBObject("loc",
                new BasicDBObject("$near",
                        new BasicDBObject("$geometry",
                                new BasicDBObject("type", "Point")
                                    .append("coordinates", myLocation))
                             .append("$maxDistance",  500)
                        )
                )
            );
System.out.println(myDoc.get("name"));
```

It should print *Central Park*. See the
[geospatial documentation]({{< docref "/reference/operator/query-geospatial">}}) for
more information.

## Text indexes

MongoDB also provides text indexes to support text search of string
content. Text indexes can include any field whose value is a string or
an array of string elements. To create a text index specify the string
literal "text" in the index document:

```java
// create a text index on the "content" field
coll.createIndex(new BasicDBObject("content", "text"));
```

As of MongoDB 2.6 text indexes are now integrated into the main query
language and enabled by default:

```java
// Insert some documents
coll.insert(new BasicDBObject("_id", 0).append("content", "textual content"));
coll.insert(new BasicDBObject("_id", 1).append("content", "additional content"));
coll.insert(new BasicDBObject("_id", 2).append("content", "irrelevant content"));

// Find using the text index
BasicDBObject search = new BasicDBObject("$search", "textual content -irrelevant");
BasicDBObject textSearch = new BasicDBObject("$text", search);
int matchCount = coll.find(textSearch).count();
System.out.println("Text search matches: "+ matchCount);

// Find using the $language operator
textSearch = new BasicDBObject("$text", search.append("$language", "english"));
matchCount = coll.find(textSearch).count();
System.out.println("Text search matches (english): "+ matchCount);

// Find the highest scoring match
BasicDBObject projection = new BasicDBObject("score", new BasicDBObject("$meta", "textScore"));
myDoc = coll.findOne(textSearch, projection);
System.out.println("Highest scoring document: "+ myDoc);
```

and it should print:

```ini
Text search matches: 2
Text search matches (english): 2
Highest scoring document: { "_id" : 1 , "content" : "additional content" , "score" : 0.75}
```

For more information about text search see the [text index]({{< docsref "/core/index-text" >}}) and
[$text query operator]({{< docsref "/reference/operator/query/text">}}) documentation.
