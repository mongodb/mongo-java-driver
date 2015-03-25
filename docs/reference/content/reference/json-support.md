+++
date = "2015-03-19T14:27:51-04:00"
draft = true
title = "JSON support"
[menu.main]
  parent = "Reference"
  weight = 50
  pre = "<i class='fa'></i>"
+++

## JSON Support

The Java driver supports reading and writing JSON documents with the [JsonReader]({{< apiref "org/bson/json/JsonReader" >}}) and
[JsonWriter]({{< apiref "org/bson/json/JsonWriter" >}}) classes, which can read/write both flavors of 
[MongoDB Extended JSON](http://docs.mongodb.org/manual/reference/mongodb-extended-json/): 

- MongoDB Extended JSON Strict Mode: representations of BSON types that conform to the [JSON RFC](http://www.json.org/). This is the 
format that [mongoexport](http://docs.mongodb.org/manual/reference/program/mongoexport/) produces and 
[mongoimport](http://docs.mongodb.org/manual/reference/program/mongoimport/) consumes.
- MongoDB Shell Mode: a superset of JSON that the 
[MongoDB shell](http://docs.mongodb.org/manual/tutorial/getting-started-with-the-mongo-shell/) can parse. 
 

## Writing JSON

Consider the task of implementing a [mongoexport](http://docs.mongodb.org/manual/reference/program/mongoexport/)-like tool using the 
Java driver.  
    
```java
String outputFilename;                 // initialize to the path of the file to write to
MongoCollection<Document> collection;  // initialize to the collection from which you want to query

BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilename));

try {
    for (Document doc : collection.find()) {
        writer.write(doc.toJson());
        writer.newLine();
} finally {
   writer.close();
}
```

The `Document.toJson()` method is the key part of this code snippet.  The implementation of this method constructs an instance of a 
`JsonWriter` with its default settings, which will write in strict mode with no new lines or indentation.

You can override this default behavior by using one of the overloads of Document.toJson().  As an example, consider the task of writing a
 JSON string that can be copied and pasted into the MongoDB shell:
 
```java
SimpleDateFormat fmt = new SimpleDateFormat("dd/MM/yy");
Date first = fmt.parse("01/01/2014");
Date second = fmt.parse("01/01/2015");
Document doc = new Document("startDate", new Document("$gt", first).append("$lt", second)); 
System.out.println(doc.toJson(new JsonWriterSettings(JsonMode.SHELL))); 
```

This code snippet will print out MongoDB shell-compatible JSON, which can then be pasted into the shell:
 
```javascript
{ "startDate" : { "$gt" : ISODate("2014-01-01T05:00:00.000Z"), "$lt" : ISODate("2015-01-01T05:00:00.000Z") } }
```

## Reading JSON

Consider the task of implementing a [mongoimport](http://docs.mongodb.org/manual/reference/program/mongoimport/)-like tool using the 
Java driver.  
    
```java
String inputFilename;                  // initialize to the path of the file to read from
MongoCollection<Document> collection;  // initialize to the collection to which you want to write

BufferedReader reader = new BufferedReader(new FileReader(inputFilename));

try {
    String json;

    while ((json = reader.readLine()) != null) {
        collection.insertOne(Document.parse(json));
    } 
} finally {
    reader.close();
}
```

The `Document.parse()` static factory method is the key part of this code snippet.  The implementation of this method constructs an 
instance of a `JsonReader` with the given string and returns an instance of an equivalent Document instance. `JsonReader`  
automatically detects the JSON flavor in the string, so you do not need to specify it. 

 



