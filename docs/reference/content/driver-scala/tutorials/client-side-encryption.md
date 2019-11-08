+++
date = "2019-06-13T09:00:01+01:00"
title = "Client Side Encryption"
[menu.main]
  parent = "Scala Tutorials"
  identifier = "Scala Sync Client Side Encryption"
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
Support for client side encryption is in beta.  Backwards-breaking changes may be made before the final release.

This guide uses the `Observable` implicits as covered in the [Quick Start Primer]({{< relref "driver-scala/getting-started/quick-start-primer.md" >}}).
{{% /note %}}

## Installation

The recommended way to get started using field level encryption in your project is with a dependency management system. 
Field level encryption requires additional packages to be installed as well as the driver itself.  
See the [installation]({{< relref "driver-scala/getting-started/installation.md" >}}) for instructions on how to install the MongoDB driver. 

{{< distroPicker >}}

### libmongocrypt

There is a separate jar file containing`libmongocrypt` bindings.

{{< install artifactId="mongodb-crypt" version="1.0.0-beta4">}}

### mongocryptd configuration

`libmongocrypt` requires the `mongocryptd` daemon / process to be running. A specific daemon / process uri can be configured in the 
`AutoEncryptionSettings` class by setting `mongocryptdURI` in the `extraOptions`.

More information about mongocryptd will soon be available from the official documentation.


### Examples

The following is a sample app that assumes the **key** and **schema** have already been created in MongoDB. The example uses a local key,
however using AWS Key Management Service is also an option. The data in the `encryptedField` field is automatically encrypted on the
insert and decrypted when using find on the client side. The following code snippet comes from the 
[`ClientSideEncryptionSimpleTour.java`]({{< srcref "driver-scala/src/it/scala/tour/ClientSideEncryptionSimpleTour.java">}}) example code
that can be found with the driver source on github:

```scala
import java.security.SecureRandom

import org.mongodb.scala.{AutoEncryptionSettings, Document, MongoClient, MongoClientSettings}
import tour.Helpers._

import scala.collection.JavaConverters._

/**
 * ClientSideEncryption Simple tour
 */
object ClientSideEncryptionSimpleTour {

  /**
   * Run this main method to see the output of this quick example.
   *
   * Requires the mongodb-crypt library in the class path and mongocryptd on the system path.
   *
   * @param args ignored args
   */
  def main(args: Array[String]): Unit = {
    val localMasterKey = new Array[Byte](96)
    new SecureRandom().nextBytes(localMasterKey)

    val kmsProviders = Map("local" -> Map[String, AnyRef]("key" -> localMasterKey).asJava).asJava

    val keyVaultNamespace = "admin.datakeys"

    val autoEncryptionSettings = AutoEncryptionSettings.builder()
      .keyVaultNamespace(keyVaultNamespace).kmsProviders(kmsProviders).build()

    val clientSettings = MongoClientSettings.builder()
      .autoEncryptionSettings(autoEncryptionSettings).build()

    val mongoClient = MongoClient(clientSettings)
    val collection = mongoClient.getDatabase("test").getCollection("coll")

    collection.drop().headResult()

    collection.insertOne(Document("encryptedField" -> "123456789")).headResult()

    collection.find().first().printHeadResult()

    // release resources
    mongoClient.close()
  }
}

```

{{% note %}}
Auto encryption is an **enterprise** only feature.
{{% /note %}}

The following example shows how to configure the `AutoEncryptionSettings` instance to create a new key and setting the json schema map.
The full code snippet can be found in 
[`ClientSideEncryptionAutoEncryptionSettingsTour.java`]({{< srcref "driver-scala/src/it/scala/tour/ClientSideEncryptionAutoEncryptionSettingsTour.java">}}):

```scala
import java.security.SecureRandom
import java.util.Base64

import scala.collection.JavaConverters._
import org.mongodb.scala._
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.model.vault.DataKeyOptions
import org.mongodb.scala.vault.ClientEncryptions
import tour.Helpers._

...

    val keyVaultNamespace = "admin.datakeys"

    val clientEncryptionSettings = ClientEncryptionSettings.builder()
      .keyVaultMongoClientSettings(
        MongoClientSettings.builder().applyConnectionString(ConnectionString("mongodb://localhost")).build())
      .keyVaultNamespace(keyVaultNamespace).kmsProviders(kmsProviders).build()

    val clientEncryption = ClientEncryptions.create(clientEncryptionSettings)

    val dataKey = clientEncryption.createDataKey("local", DataKeyOptions()).headResult()

    val base64DataKeyId = Base64.getEncoder.encodeToString(dataKey.getData)
    val dbName = "test"
    val collName = "coll"
    val autoEncryptionSettings = AutoEncryptionSettings.builder()
      .keyVaultNamespace(keyVaultNamespace)
      .kmsProviders(kmsProviders)
      .schemaMap(Map(s"$dbName.$collName" -> BsonDocument(
        s"""{
            properties: {
              encryptedField: {
                encrypt: {
                  keyId: [{
                    "$$binary": {
                      "base64": "$base64DataKeyId",
                      "subType": "04"
                    }
                  }],
                  bsonType: "string",
                  algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                }
              }
            },
            bsonType: "object"
          }""")).asJava)
      .build()
```

**Coming soon:** An example using the community version and demonstrating explicit encryption/decryption.
