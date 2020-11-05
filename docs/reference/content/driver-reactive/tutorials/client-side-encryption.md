+++
date = "2019-06-13T09:00:01+01:00"
title = "Client Side Encryption"
[menu.main]
  parent = "Reactive Tutorials"
  identifier = "Reactive Sync Client Side Encryption"
  weight = 16
  pre = "<i class='fa fa-lock'></i>"
+++

# Client Side Encryption

New in MongoDB 4.2 client side encryption allows administrators and developers to encrypt specific data fields in addition to other
MongoDB encryption features.

With field level encryption, developers can encrypt fields client side without any server-side 
configuration or directives. Client-side field level encryption supports workloads where applications must guarantee that 
unauthorized parties, including server administrators, cannot read the encrypted data.

{{% note class="important" %}}
This guide uses the `Subscriber` implementations as covered in the [Quick Start Primer]({{< relref "driver-reactive/getting-started/quick-start-primer.md" >}}).
{{% /note %}}
## Installation

The recommended way to get started using field level encryption in your project is with a dependency management system. 
Field level encryption requires additional packages to be installed as well as the driver itself.  
See the [installation]({{< relref "driver-reactive/getting-started/installation.md" >}}) for instructions on how to install the MongoDB driver. 

{{< distroPicker >}}

### libmongocrypt

There is a separate jar file containing`libmongocrypt` bindings.

{{< install artifactId="mongodb-crypt" version="1.1.0-beta1">}}

### mongocryptd configuration

`libmongocrypt` requires the `mongocryptd` daemon / process to be running. A specific daemon / process uri can be configured in the 
`AutoEncryptionSettings` class by setting `mongocryptdURI` in the `extraOptions`.

More information about mongocryptd will soon be available from the official documentation.


### Examples

The following is a sample app that assumes the **key** and **schema** have already been created in MongoDB. The example uses a local key,
however using either of the AWS / Azure / GCP Key Management Service is also an option. The data in the `encryptedField` field is 
automatically encrypted on the insert and decrypted when using find on the client side. The following code snippet comes from the 
[`ClientSideEncryptionSimpleTour.java`]({{< srcref "driver-reactive/src/examples/reactivestreams/tour/ClientSideEncryptionSimpleTour.java" >}}) example code
that can be found with the driver source on github:

```java
import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

public class ClientSideEncryptionSimpleTour {

    public static void main(final String[] args) {

        // This would have to be the same master key as was used to create the encryption key
        final byte[] localMasterKey = new byte[96];
        new SecureRandom().nextBytes(localMasterKey);

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
           put("local", new HashMap<String, Object>() {{
               put("key", localMasterKey);
           }});
        }};

        String keyVaultNamespace = "admin.datakeys";

        AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
                .keyVaultNamespace(keyVaultNamespace)
                .kmsProviders(kmsProviders)
                .build();

        MongoClientSettings clientSettings = MongoClientSettings.builder()
                .autoEncryptionSettings(autoEncryptionSettings)
                .build();

        MongoClient mongoClient = MongoClients.create(clientSettings);
        MongoCollection<Document> collection = mongoClient.getDatabase("test").getCollection("coll");

        ObservableSubscriber<Void> successSubscriber = new OperationSubscriber<>();
        collection.drop().subscribe(successSubscriber);
        successSubscriber.await();

        successSubscriber = new OperationSubscriber<>();
        collection.insertOne(new Document("encryptedField", "123456789")).subscribe(successSubscriber);
        successSubscriber.await();

        ObservableSubscriber<Document> documentSubscriber = new PrintDocumentSubscriber();
        collection.find().first().subscribe(documentSubscriber);
        documentSubscriber.await();
    }
}
```

{{% note %}}
Auto encryption is an **enterprise** only feature.
{{% /note %}}

The following example shows how to configure the `AutoEncryptionSettings` instance to create a new key and setting the json schema map.
The full code snippet can be found in 
[`ClientSideEncryptionAutoEncryptionSettingsTour.java`]({{< srcref "driver-reactive/src/examples/reactivestreams/tour/ClientSideEncryptionAutoEncryptionSettingsTour.java" >}}):

```java
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ConnectionString;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import org.bson.BsonBinary;
import org.bson.BsonDocument;

import java.util.Base64;

...

String keyVaultNamespace = "admin.datakeys";
ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
        .keyVaultMongoClientSettings(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://localhost"))
                .build())
        .keyVaultNamespace(keyVaultNamespace)
        .kmsProviders(kmsProviders)
        .build();

ClientEncryption clientEncryption = ClientEncryptions.create(clientEncryptionSettings);
BsonBinary dataKeyId = clientEncryption.createDataKey("local", new DataKeyOptions());
final String base64DataKeyId = Base64.getEncoder().encodeToString(dataKeyId.getData());

final String dbName = "test";
final String collName = "coll";
AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
        .keyVaultNamespace(keyVaultNamespace)
        .kmsProviders(kmsProviders)
        .schemaMap(new HashMap<String, BsonDocument>() {{
            put(dbName + "." + collName,
                    // Need a schema that references the new data key
                    BsonDocument.parse("{"
                            + "  properties: {"
                            + "    encryptedField: {"
                            + "      encrypt: {"
                            + "        keyId: [{"
                            + "          \"$binary\": {"
                            + "            \"base64\": \"" + base64DataKeyId + "\","
                            + "            \"subType\": \"04\""
                            + "          }"
                            + "        }],"
                            + "        bsonType: \"string\","
                            + "        algorithm: \"AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic\""
                            + "      }"
                            + "    }"
                            + "  },"
                            + "  \"bsonType\": \"object\""
                            + "}"));
        }}).build();
```

#### Explicit Encryption and Decryption
Explicit encryption and decryption is a **MongoDB community** feature and does not use the `mongocryptd` process. Explicit encryption is 
provided by the `ClientEncryption` class. 
The full code snippet can be found in [`ClientSideEncryptionExplicitEncryptionAndDecryptionTour.java`]({{< srcref "driver-reactive-streams/src/examples/reactivestreams/tour/ClientSideEncryptionAutoEncryptionSettingsTour.java" >}}):

```
// This would have to be the same master key as was used to create the encryption key
final byte[] localMasterKey = new byte[96];
new SecureRandom().nextBytes(localMasterKey);

Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
    put("local", new HashMap<String, Object>() {{
        put("key", localMasterKey);
    }});
}};

MongoNamespace keyVaultNamespace = new MongoNamespace("encryption.testKeyVault");

MongoClientSettings clientSettings = MongoClientSettings.builder().build();
MongoClient mongoClient = MongoClients.create(clientSettings);

// Set up the key vault for this example
MongoCollection<Document> keyVaultCollection = mongoClient
   .getDatabase(keyVaultNamespace.getDatabaseName())
   .getCollection(keyVaultNamespace.getCollectionName());

ObservableSubscriber<Void> successSubscriber = new OperationSubscriber<>();
keyVaultCollection.drop().subscribe(successSubscriber);
successSubscriber.await();

// Ensure that two data keys cannot share the same keyAltName.
ObservableSubscriber<String> indexSubscriber = new OperationSubscriber<>();
keyVaultCollection.createIndex(Indexes.ascending("keyAltNames"),
        new IndexOptions().unique(true)
                .partialFilterExpression(Filters.exists("keyAltNames")))
        .subscribe(indexSubscriber);
indexSubscriber.await();

MongoCollection<Document> collection = mongoClient.getDatabase("test").getCollection("coll");
successSubscriber = new OperationSubscriber<>();
collection.drop().subscribe(successSubscriber);
successSubscriber.await();

// Create the ClientEncryption instance
ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
        .keyVaultMongoClientSettings(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://localhost"))
                .build())
        .keyVaultNamespace(keyVaultNamespace.getFullName())
        .kmsProviders(kmsProviders)
        .build();

ClientEncryption clientEncryption = ClientEncryptions.create(clientEncryptionSettings);

BsonBinary dataKeyId = clientEncryption.createDataKey("local", new DataKeyOptions());

// Explicitly encrypt a field
BsonBinary encryptedFieldValue = clientEncryption.encrypt(new BsonString("123456789"),
        new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").keyId(dataKeyId));

ObservableSubscriber<InsertOneResult> insertOneSubscriber = new OperationSubscriber<>();
collection.insertOne(new Document("encryptedField", encryptedFieldValue))
    .subscribe(insertOneSubscriber);
insertOneSubscriber.await();

ObservableSubscriber<Document> documentSubscriber = new OperationSubscriber<>();
collection.find().first().subscribe(documentSubscriber);

Document doc = documentSubscriber.get().get(0);
System.out.println(doc.toJson());

// Explicitly decrypt the field
System.out.println(
    clientEncryption.decrypt(new BsonBinary(doc.get("encryptedField", Binary.class).getData()))
);
```
#### Explicit Encryption and Auto Decryption

Although automatic encryption requires MongoDB 4.2 enterprise or a MongoDB 4.2 Atlas cluster, automatic decryption is supported for all 
users. To configure automatic decryption without automatic encryption set `bypassAutoEncryption(true)`. The full code snippet can be found in [`ClientSideEncryptionExplicitEncryptionOnlyTour.java`]({{< srcref "driver-reactive-streams/src/examples/reactivestreams/tour/ClientSideEncryptionExplicitEncryptionOnlyTour.java" >}}):

```
...
MongoClientSettings clientSettings = MongoClientSettings.builder()
    .autoEncryptionSettings(AutoEncryptionSettings.builder()
            .keyVaultNamespace(keyVaultNamespace.getFullName())
            .kmsProviders(kmsProviders)
            .bypassAutoEncryption(true)
            .build())
    .build();
MongoClient mongoClient = MongoClients.create(clientSettings);

...

// Explicitly encrypt a field
BsonBinary encryptedFieldValue = clientEncryption.encrypt(new BsonString("123456789"),
        new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").keyId(dataKeyId));

ObservableSubscriber<InsertOneResult> insertOneSubscriber = new OperationSubscriber<>();
collection.insertOne(new Document("encryptedField", encryptedFieldValue))
    .subscribe(insertOneSubscriber);
insertOneSubscriber.await();

ObservableSubscriber<Document> documentSubscriber = new OperationSubscriber<>();
collection.find().first().subscribe(documentSubscriber);

Document doc = documentSubscriber.get().get(0);
System.out.println(doc.toJson());
```