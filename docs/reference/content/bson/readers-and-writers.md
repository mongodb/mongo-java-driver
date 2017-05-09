+++
date = "2015-03-19T14:27:51-04:00"
title = "Readers and Writers"
[menu.main]
  parent = "BSON"
  weight = 30
  pre = "<i class='fa'></i>"
+++

## BsonWriter and BsonReader

The various implementations of the `Bson` interface discussed in the previous section all represent BSON documents using an underlying 
Java `Map` instance. However, they are not directly responsible for reading and writing their representations from and to BSON.  Instead, 
this process is delegated to [`BsonWriter`]({{< apiref "org/bson/BsonWriter" >}}) and 
[`BsonReader`]({{< apiref "org/bson/BsonReader" >}}), abstract classes that expose methods for iterative, stream-based processing of 
BSON documents. 
  
### BsonWriter

The `BsonWriter` class exposes methods for writing a BSON document.  Consider the task of writing the document 

```javascript
{
  "_id" : ObjectId("..."),
  "name": "Steve",
  "jobTitle": "Electrician",
  "dateStarted": "5/19/2016",
  "numberOfJobs": 5
}
```

The following code will stream a document of this structure to a `BsonWriter`:

```java
BsonWriter writer = ... // Construct a BsonWriter

writer.writeStartDocument();
    writer.writeName("_id");
    writer.writeObjectId(new ObjectId());
    writer.writeName("name");
    writer.writeString("Steve");
    writer.writeName("jobTitle");
    writer.writeString("Electrician");
    writer.writeName("dateStarted");
    writer.writeDateTime(new SimpleDateFormat("M/d/yyyy").parse("5/19/2016").getTime());
    writer.writeName("numberOfJobs");
    writer.writeInt32(5);
writer.writeEndDocument();
```

The indentation is not necessary: it's just to clarify that the stream of events written to the `BsonWriter`, although written 
iteratively, have an implicit hierarchical structure.  The `BsonWriter` validates that the events create only properly structured BSON 
documents.  Otherwise, it throws a `BsonSerializationException`.

The two most important classes to extend `BsonWriter` are [`BsonBinaryWriter`]({{< apiref "org/bson/BsonBinaryWriter" >}}) and 
[`JsonWriter`]({{< apiref "org/bson/json/JsonWriter" >}}).  `BsonBinaryWriter` writes the BSON 
document as a stream of bytes in accordance with the [BSON](http://www.bsonspec.org) specification, while `JsonWriter` writes the BSON 
document as a stream of characters in accordance with 
[MongoDB Extended JSON](http://docs.mongodb.org/manual/reference/mongodb-extended-json/).
 
### BsonReader

The `BsonReader` class exposes methods for reading a BSON document.  Consider the task of reading the document written above with a 
`BsonReader`: 
 
```java
BsonReader reader = ... // Construct a BsonReader

reader.readStartDocument();
    reader.readName();      // read the name "_id"
    reader.readObjectId();  // read the object id
    reader.readName();      // read the name "name"
    reader.readString();    // read string "Steve"
    reader.readName();      // read the name "jobTitle"
    reader.readString();    // read string "Electrician"
    reader.readName();      // read the name "dateStarted"
    reader.readDateTime();  // read date "5/19/2016"
    reader.readName();      // read the name "numberOfJobs"
    reader.readInt32();     // read the integer 5
reader.readEndDocument();
```

As with the writer example, the indentation is not necessary: it's just to clarify that the stream of events read from the `BsonReader`, 
although written iteratively, have an implicit hierarchical structure.  The `BsonReader` will throw a `BsonSerializationException` if the 
events read do not match the structure of the document that is being read from.  

### An Example with BSON Array

Suppose the application developer has now decided to include a list of testimonials for each worker. Consider the following document:

```javascript
{
  "_id" : ObjectId("..."),
  "name": "Steve",
  "jobTitle": "Electrician",
  "dateStarted": "5/19/2016",
  "numberOfJobs": 5,
  "testimonials": [
                    { "authorEmail" : "joe@example.com",
                      "comment": "Excellent electrician!"
                    },
                    { "authorEmail" : "jane@example.com",
                      "comment": "Thank you for great work!"
                    }
                  ]
}
```

This following code will stream a document of this structure to a `BsonWriter`:

```java
BsonWriter writer = ... // Construct a BsonWriter

writer.writeStartDocument();
    writer.writeString("name", "Steve");
    writer.writeString("jobTitle", "Electrician");
    writer.writeDateTime("dateStarted", new SimpleDateFormat("M/d/yyyy").parse("5/19/2016").getTime());
    writer.writeInt32("numberOfJobs", 5);
    writer.writeName("testimonials");
    writer.writeStartArray();
        writer.writeStartDocument();
            writer.writeString("authorEmail", "joe@example.com");
            writer.writeString("comment", "Excellent electrician!");
        writer.writeEndDocument();
        writer.writeStartDocument();
            writer.writeString("authorEmail", "jane@example.com");
            writer.writeString("comment", "Thank you for great work!");
        writer.writeEndDocument();
    writer.writeEndArray();
writer.writeEndDocument();
```

In the above code, note the passing of element names to write methods. For each value type, `BsonWriter` provides two write methods: 
one that accepts just one parameter, the value of the element. The second method accepts both element name and element value. Using 
the second variation can help in writing more compact code.

The document written above can be read with `BsonReader` using the following code:

```java
BsonReader reader = ... // Construct a BsonReader

reader.readStartDocument();
    reader.readObjectId("_id");
    reader.readString("name");
    reader.readString("jobTitle");
    reader.readDateTime("dateStarted");
    reader.readInt32("numberOfJobs");
    reader.readStartArray();
        reader.readStartDocument();
            reader.readString("authorEmail");
            reader.readString("comment");
        reader.readEndDocument();
        reader.readStartDocument();
            reader.readString("authorEmail");
            reader.readString("comment");
        reader.readEndDocument();
    reader.readEndArray();
reader.readEndDocument();
```

In most situations an application will not know the exact structure of the document being read.  For that reason, `BsonReader`
exposes a few methods that allow an application to peek ahead so that it can figure out what method to call next.  Consider a situation 
where an application must read a BSON document with an unknown structure:
 
```java
reader.readStartDocument();

while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
    String fieldName = reader.readName();
    switch (reader.getCurrentBsonType()) {
        case INT32:
            int intValue = reader.readInt32();
            break;
        case INT64:
            long longValue = reader.readInt64();
            break;
        // ... handle each supported field type
    }
}

reader.readEndDocument();
``` 

In this example, the application iterates through the fields of the document until it reaches `END_OF_DOCUMENT`.  For each field, it 
reads the name and then the value based on the `BsonType` of the field.

A similar pattern can be used to read a BSON array:

```java
reader.readStartArray();

while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
    switch (reader.getCurrentBsonType()) {
        case INT32:
            int intValue = reader.readInt32();
            break;
        case INT64:
            long longValue = reader.readInt64();
            break;
        // ... handle each supported field type
    }
}

reader.readEndArray();
``` 

The only significant difference between reading an array and reading a document is that, since the elements of an array do not have names,
there is no field name to read, only a series of values.

The two most important classes to extend `BsonReader` are [`BsonBinaryReader`]({{< apiref "org/bson/BsonBinaryReader" >}}) and 
[`JsonReader`]({{< apiref "org/bson/json/JsonReader" >}}). `BsonBinaryReader` reads the BSON 
document as a stream of bytes in accordance with the [BSON](http://www.bsonspec.org) specification, while `JsonReader` reads the BSON 
document as a stream of characters in accordance with 
[MongoDB Extended JSON](http://docs.mongodb.org/manual/reference/mongodb-extended-json/).                              
