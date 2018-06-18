## The MongoDB Asynchronous Java Driver

A callback-based fully non-blocking and asynchronous I/O operations MongoDB Java driver.

## Binaries

Binaries and dependency information for Maven, Gradle, Ivy and others can be found at
[http://search.maven.org](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.mongodb%22%20AND%20a%3A%22mongodb-driver-async%22).

Example for Maven:

```xml
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-async</artifactId>
    <version>x.y.z</version>
</dependency>
```

Snapshot builds are also published regulary via Sonatype.

Example for Maven:

```xml
    <repositories>
        <repository>
            <id>sonatype-snapshot</id>
            <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
        </repository>
    </repositories>
```


## Usage example:

```java

    import com.mongodb.Block;
    import com.mongodb.ConnectionString;
    import com.mongodb.async.SingleResultCallback;
    import com.mongodb.async.client.MongoClient;
    import com.mongodb.async.client.MongoClients;
    import com.mongodb.async.client.MongoCollection;
    import com.mongodb.async.client.MongoDatabase;
    import org.bson.Document;

    import java.util.ArrayList;
    import java.util.List;
    import java.util.concurrent.CountDownLatch;
    import java.util.concurrent.ExecutionException;
    import java.util.concurrent.TimeUnit;
    import java.util.concurrent.atomic.AtomicLong;

    // Open the client
    MongoClient mongoClient = MongoClients.create(new ConnectionString("mongodb://localhost:27017"));

    // Print out all databases
    final CountDownLatch listDbsLatch = new CountDownLatch(1);
    System.out.println("Outputting database names:");
    mongoClient.listDatabaseNames().forEach(new Block<String>() {
        @Override
        public void apply(final String name) {
            System.out.println(" - " + name);
        }
    }, new SingleResultCallback<Void>() {
        @Override
        public void onResult(final Void result, final Throwable t) {
            listDbsLatch.countDown();
        }
    });
    boolean listedAllDbs = listDbsLatch.await(1, TimeUnit.SECONDS);
    assert(listedAllDbs);


    // get handle to "mydb" database
    final MongoDatabase database = mongoClient.getDatabase("mydb");

    // Get handle to "test" collection
    final MongoCollection<Document> collection = database.getCollection("test");

    // Drop the collection and insert 100 documents
    final CountDownLatch insertLatch = new CountDownLatch(1);
    System.out.println("Dropping collection and inserting documents");
    collection.dropCollection(new SingleResultCallback<Void>() {
        @Override
        public void onResult(final Void result, final Throwable t) {
            System.out.println(" - collection dropped");
            // now, lets add lots of little documents to the collection so we can explore queries and cursors
            List<Document> documents = new ArrayList<Document>();
            for (int i = 0; i < 100; i++) {
                documents.add(new Document("i", i));
            }
            collection.insertMany(documents, new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    System.out.println(" - documents inserted");
                    insertLatch.countDown();
                }
            });
        }
    });

    boolean inserted = insertLatch.await(10, TimeUnit.SECONDS);
    assert(inserted);

    // Count should now be 100
    final CountDownLatch countLatch = new CountDownLatch(1);
    final AtomicLong count = new AtomicLong();
    System.out.println("Counting the number of documents");
    collection.countDocuments(new SingleResultCallback<Long>() {
        @Override
        public void onResult(final Long result, final Throwable t) {
            count.set(result);
            countLatch.countDown();
        }
    });
    boolean counted = countLatch.await(1, TimeUnit.SECONDS);
    assert(counted);
    System.out.println(" - Count result: " + count.get());

    // release resources
    mongoClient.close();

```
