+++
date = "2016-06-07T23:28:50-04:00"
title = "Text Search"
[menu.main]
parent = "Sync Tutorials"
identifier = "Text Search"
weight = 80
pre = "<i class='fa'></i>"
+++

## Text Search

MongoDB supports query operations that perform a [text search]({{<docsref "text-search">}}) of string content. To perform text search, MongoDB uses a [text index]({{<docsref "core/index-text">}}) and the [`$text` query operator]({{<docsref "reference/operator/query/text">}}).

The Java driver provides the [`Filters.text()`]({{<apiref "com/mongodb/client/model/Filters.html#text-java.lang.String-com.mongodb.client.model.TextSearchOptions-">}}) helper to facilitate the creation of text search query filters.

## Prerequisites

- The example below requires a ``restaurants`` collection in the ``test`` database. To create and populate the collection, follow the directions in [github] (https://github.com/mongodb/docs-assets/tree/drivers).

- Include the following import statements:

     ```java
     import com.mongodb.Block;
     import com.mongodb.MongoClient;
     import com.mongodb.client.MongoCollection;
     import com.mongodb.client.MongoDatabase;

     import com.mongodb.client.model.Indexes;
     import com.mongodb.client.model.Filters;
     import com.mongodb.client.model.Sorts;
     import com.mongodb.client.model.TextSearchOptions;
     import com.mongodb.client.model.Projections;
     import org.bson.Document;
     ```

- Include the following code which the examples in the tutorials will use to print the results of the text search:

     ```java
     Block<Document> printBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };
     ```

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` instance.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017` and define `database` to refer to the `test` database:

```java
MongoClient mongoClient = new MongoClient();
MongoDatabase database = mongoClient.getDatabase("test");
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md">}}).

## Create the `text` Index

To create a [text index]({{<docsref "core/index-text">}}), use the [`Indexes.text`]({{< relref "builders/indexes.md#text-index">}})
static helper to create a specification for a text index and pass to [`MongoCollection.createIndex()`]({{<apiref "com/mongodb/client/MongoCollection.html#createIndex-org.bson.conversions.Bson-">}}) method.

The following example creates a text index on the `name` field for the `restaurants` collection.

```java
MongoCollection<Document> collection = database.getCollection("restaurants");
collection.createIndex(Indexes.text("name"));
```

## Perform Text Search

To perform text search, use the [`Filters.text()`]({{<apiref "com/mongodb/client/model/Filters.html#text-java.lang.String-com.mongodb.client.model.TextSearchOptions-">}}) helper to specify the text search query filter.

For example, the following code performs a text search on the `name` field for the word `"bakery"` or `"coffee"`.

```java
long matchCount = collection.count(Filters.text("bakery coffee"));
System.out.println("Text search matches: " + matchCount);
```

The example should print the following output:

```json
Text search matches: 2
```

For more information on the text search, see [`$text` operator]({{<docsref "reference/operator/query/text">}}).

### Text Score

For each matching document, text search assigns a score, representing the relevance of a document to the specified text search query filter. To return and sort by score, use the [`$meta`]({{<docsref "reference/operator/query/text/#sort-by-text-search-score">}}) operator in the projection document and the sort expression.


```java
collection.find(Filters.text("bakery cafe"))
                              .projection(Projections.metaTextScore("score"))
                              .sort(Sorts.metaTextScore("score")).forEach(printBlock);
```

The example should print the following output:



### Specify a Text Search Option

The  [`Filters.text()`]({{<apiref "com/mongodb/client/model/Filters.html#text-java.lang.String-com.mongodb.client.model.TextSearchOptions-">}}) helper can accept various [text search options]({{<docsref "reference/operator/query/text">}}). The Java driver provides [`TextSearchOptions`]({{<apiref "com/mongodb/client/model/TextSearchOptions.html">}}) class to specify these options.

For example, the following text search specifies the [text search language]({{<docsref "reference/text-search-languages">}}) option when performing text search for the word `cafe`:

```java
long matchCountEnglish = collection.count(Filters.text("cafe", new TextSearchOptions().language("english")));
System.out.println("Text search matches (english): " + matchCountEnglish);
```

The example should print the following output:

```json
Text search matches (english): 1
```

For more information about text search see the following sections in the MongoDB Server Manual:

- [`$text` query operator]({{< docsref "reference/operator/query/text">}})

- [`text` index]({{< docsref "core/index-text" >}})

- [Text Search Languages]({{<docsref "reference/text-search-languages">}})
