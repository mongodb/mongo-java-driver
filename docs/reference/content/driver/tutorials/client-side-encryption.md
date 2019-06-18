+++
date = "2019-06-13T09:00:01+01:00"
title = "Client Site Encryption"
[menu.main]
  parent = "Sync Tutorials"
  identifier = "Sync Client Side Encryption"
  weight = 16
  pre = "<i class='fa fa-lock'></i>"
+++

# Client Side Encryption

New in MongoDB 4.2 client side encryption allows administrators and developers to encrypt specific data fields in addition to other
MongoDB encryption features.

With field level encryption, developers can encrypt fields client side without any server-side 
configuration or directives. Client-side field level encryption supports workloads where applications must guarantee that 
unauthorized parties, including server administrators, cannot read the encrypted data.

## Installation

The recommended way to get started using field level encryption in your project is with a dependency management system. 
Field level encryption requires additional packages to be installed as well as the driver itself.  
See the [installation]({{< relref "driver/getting-started/installation.md" >}}) for instructions on how to install the MongoDB driver. 

{{< distroPicker >}}

### libmongocrypt

There is a separate jar file containing`libmongocrypt` bindings.

{{< install artifactId="mongodb-mongocrypt" version="1.0.0-beta1">}}

If the jar fails to run there are separate jar files for specific architectures:

#### RHEL 7.0*
{{< install artifactId="mongodb-crypt" version="1.0.0-beta1" classifier="linux64-rhel70">}}

#### OSX*
{{< install artifactId="mongodb-crypt" version="1.0.0-beta1" classifier="osx">}}

#### Windows*
{{< install artifactId="mongodb-crypt" version="1.0.0-beta1" classifier="win64">}}

#### Ubuntu 16.04
{{< install artifactId="mongodb-crypt" version="1.0.0-beta1" classifier="linux64-ubuntu1604">}}


* Distribution is included in the main `mongodb-crypt` jar file.

### mongocryptd configuration

`libmongocrypt` requires the `mongocryptd` daemon / process to be running. A specific daemon / process uri can be configured in the 
`AutoEncryptionSettings` class by setting `mongocryptdURI` in the `extraOptions`.

More information about libmongocrypt will soon be available from the official documentation.


### Examples

The following is a sample app that assumes key and schema have already been created in MongoDB. The example uses a local key,
however using AWS Key Management Service is also an option. The data in the `encryptedField` field is automatically encrypted on the
insert and decrypted when using find on the client side:

```java
import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import org.bson.Document;

import java.security.SecureRandom;
import java.util.Map;

public class ClientSideEncryptionSimpleTest {

    public static void main(String[] args) {

        // This would have to be the same master key as was used to create the encryption key
        var localMasterKey = new byte[96];
        new SecureRandom().nextBytes(localMasterKey);

        var kmsProviders = Map.of("local", Map.<String, Object>of("key", localMasterKey));
        var keyVaultNamespace = "admin.datakeys";

        var autoEncryptionSettings = AutoEncryptionSettings.builder()
            .keyVaultNamespace(keyVaultNamespace)
            .kmsProviders(kmsProviders)
            .build();

        var clientSettings = MongoClientSettings.builder()
            .autoEncryptionSettings(autoEncryptionSettings)
            .build();

        var client = MongoClients.create(clientSettings);
        var collection = client.getDatabase("test").getCollection("coll");
        collection.drop(); // Clear old data

        collection.insertOne(new Document("encryptedField", "123456789"));

        System.out.println(collection.find().first().toJson());
    }
}
```

{{% note %}}
Auto encryption is an **enterprise** only feature.
{{% /note %}}

The following example shows how to configure the `AutoEncryptionSettings` instance to create a new key and setting the json schema map:

```java
import com.mongodb.ConnectionString;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.client.vault.ClientEncryptions;

...


var keyVaultNamespace = "admin.datakeys";
var clientEncryptionSettings = ClientEncryptionSettings.builder()
        .keyVaultMongoClientSettings(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://localhost"))
                .build())
        .keyVaultNamespace(keyVaultNamespace)
        .kmsProviders(kmsProviders)
        .build();

var clientEncryption = ClientEncryptions.create(clientEncryptionSettings);
var dataKeyId = keyVault.createDataKey("local", new DataKeyOptions());
var base64DataKeyId = Base64.getEncoder().encodeToString(dataKeyId.getData());

var dbName = "test";
var collName = "coll";
var autoEncryptionSettings = AutoEncryptionSettings.builder()
    .keyVaultNamespace(keyVaultNamespace)
    .kmsProviders(kmsProviders)
    .namespaceToLocalSchemaDocumentMap(Map.of(dbName + "." + collName,
        // Need a schema that references the new data key
        BsonDocument.parse("{" +
                "  properties: {" +
                "    encryptedField: {" +
                "      encrypt: {" +
                "        keyId: [{" +
                "          \"$binary\": {" +
                "            \"base64\": \"" + base64DataKeyId + "\"," +
                "            \"subType\": \"04\"" +
                "          }" +
                "        }]," +
                "        bsonType: \"string\"," +
                "        algorithm: \"AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic\"" +
                "      }" +
                "    }" +
                "  }," +
                "  \"bsonType\": \"object\"" +
                "}"))
    ).build();
```

{{% note %}}
Auto encryption is an **enterprise** only feature.
{{% /note %}}

**Coming soon:** An example using the community version and demonstrating explicit encryption/decryption.
