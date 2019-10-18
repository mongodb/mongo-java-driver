/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client

import com.mongodb.AutoEncryptionSettings
import com.mongodb.ClientEncryptionSettings
import com.mongodb.MongoClientException
import com.mongodb.MongoNamespace
import com.mongodb.WriteConcern
import com.mongodb.client.model.vault.DataKeyOptions
import com.mongodb.client.model.vault.EncryptOptions
import com.mongodb.client.vault.ClientEncryption
import com.mongodb.client.vault.ClientEncryptions
import com.mongodb.event.CommandStartedEvent
import com.mongodb.internal.connection.TestCommandListener
import org.bson.BsonBinarySubType
import org.bson.BsonDocument
import org.bson.BsonString

import static com.mongodb.ClusterFixture.isNotAtLeastJava8
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.client.Fixture.getDefaultDatabaseName
import static com.mongodb.client.Fixture.getMongoClient
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder
import static com.mongodb.client.model.Filters.eq
import static java.util.Collections.singletonMap
import static org.junit.Assume.assumeFalse
import static org.junit.Assume.assumeTrue

class ClientSideEncryptionProseTestSpecification extends FunctionalSpecification {

    private final MongoNamespace keyVaultNamespace = new MongoNamespace('test.datakeys')
    private final MongoNamespace autoEncryptingCollectionNamespace = new MongoNamespace(getDefaultDatabaseName(),
            'ClientSideEncryptionProseTestSpecification')
    private final MongoCollection dataKeyCollection = getMongoClient()
            .getDatabase(keyVaultNamespace.databaseName).getCollection(keyVaultNamespace.collectionName, BsonDocument)
    private final MongoCollection<BsonDocument> dataCollection = getMongoClient()
            .getDatabase(autoEncryptingCollectionNamespace.databaseName).getCollection(autoEncryptingCollectionNamespace.collectionName,
            BsonDocument)

    private MongoClient autoEncryptingClient
    private ClientEncryption clientEncryption
    private MongoCollection<BsonDocument> autoEncryptingDataCollection
    private TestCommandListener commandListener

    def setup() {
        assumeFalse(isNotAtLeastJava8())
        assumeTrue(serverVersionAtLeast(4, 2))
        assumeTrue('Key vault tests disabled',
                System.getProperty('org.mongodb.test.awsAccessKeyId') != null
                        && !System.getProperty('org.mongodb.test.awsAccessKeyId').isEmpty())
        dataKeyCollection.drop()
        dataCollection.drop()

        def providerProperties =
                ['local': ['key': Base64.getDecoder().decode('Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN'
                        + '3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk')],
                 'aws'  : ['accessKeyId'    : System.getProperty('org.mongodb.test.awsAccessKeyId'),
                           'secretAccessKey': System.getProperty('org.mongodb.test.awsSecretAccessKey')]
                ]

        autoEncryptingClient = MongoClients.create(getMongoClientSettingsBuilder()
                .autoEncryptionSettings(AutoEncryptionSettings.builder()
                        .keyVaultNamespace(keyVaultNamespace.fullName)
                        .kmsProviders(providerProperties)
                        .schemaMap(singletonMap(autoEncryptingCollectionNamespace.fullName,
                                BsonDocument.parse(
                                        '''
    {
      "bsonType": "object",
      "properties": {
        "encrypted_placeholder": {
          "encrypt": {
            "keyId": "/placeholder",
            "bsonType": "string",
            "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
          }
        }
      }
    }''')))
                        .build())
                .build())

        autoEncryptingDataCollection = autoEncryptingClient.getDatabase(autoEncryptingCollectionNamespace.databaseName)
                .getCollection(autoEncryptingCollectionNamespace.collectionName, BsonDocument)

        commandListener = new TestCommandListener()
        clientEncryption = ClientEncryptions.create(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettingsBuilder()
                        .addCommandListener(commandListener)
                        .build())
                .keyVaultNamespace(keyVaultNamespace.fullName)
                .kmsProviders(providerProperties)
                .build())
    }

    def 'client encryption prose test'() {
        when:
        def localDataKeyId = clientEncryption.createDataKey('local', new DataKeyOptions().keyAltNames(['local_altname']))

        then:
        commandListener.getCommandStartedEvents().size() == 1
        def event = commandListener.getCommandStartedEvents().get(0) as CommandStartedEvent
        event.getCommand().containsKey('writeConcern')
        event.getCommand().getDocument('writeConcern') == WriteConcern.MAJORITY.asDocument()

        localDataKeyId != null
        localDataKeyId.type == BsonBinarySubType.UUID_STANDARD.value
        dataKeyCollection.find(eq('masterKey.provider', 'local')).into([]).size() == 1

        when:
        def localEncrypted = clientEncryption.encrypt(new BsonString('hello local'),
                new EncryptOptions('AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic')
                .keyId(localDataKeyId))

        then:
        localEncrypted.asBinary().getType() == (byte) 6

        when:
        autoEncryptingDataCollection.insertOne(new BsonDocument('_id', new BsonString('local'))
                .append('value', localEncrypted))

        then:
        autoEncryptingDataCollection.find(eq('_id', new BsonString('local'))).first().getString('value')
                .value == 'hello local'

        when:
        def localEncryptedWithAltName = clientEncryption.encrypt(new BsonString('hello local'),
                new EncryptOptions('AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic')
                .keyAltName('local_altname'))

        then:
        localEncryptedWithAltName == localEncrypted

        when:
        def awsDataKeyId = clientEncryption.createDataKey('aws',
                new DataKeyOptions().keyAltNames(['aws_altname'])
                .masterKey(new BsonDocument('region', new BsonString('us-east-1'))
                .append('key', new BsonString('arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0'))))

        then:
        awsDataKeyId != null
        awsDataKeyId.type == BsonBinarySubType.UUID_STANDARD.value
        dataKeyCollection.find(eq('masterKey.provider', 'aws')).into([]).size() == 1

        when:
        def awsEncrypted = clientEncryption.encrypt(new BsonString('hello aws'),
                new EncryptOptions('AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic')
                        .keyId(awsDataKeyId))

        then:
        awsEncrypted.asBinary().getType() == (byte) 6

        when:
        autoEncryptingDataCollection.insertOne(new BsonDocument('_id', new BsonString('aws'))
                .append('value', awsEncrypted))

        then:
        autoEncryptingDataCollection.find(eq('_id', new BsonString('aws'))).first().getString('value')
                .value == 'hello aws'

        when:
        def awsEncryptedWithAltName = clientEncryption.encrypt(new BsonString('hello aws'),
                new EncryptOptions('AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic')
                        .keyAltName('aws_altname'))

        then:
        awsEncryptedWithAltName == awsEncrypted

        when:
        autoEncryptingDataCollection.insertOne(new BsonDocument('encrypted_placeholder', localEncrypted))

        then:
        thrown(MongoClientException)
    }
}
