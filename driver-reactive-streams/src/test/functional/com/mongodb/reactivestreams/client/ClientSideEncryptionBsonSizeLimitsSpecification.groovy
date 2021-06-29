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

package com.mongodb.reactivestreams.client

import com.mongodb.AutoEncryptionSettings
import com.mongodb.ClientEncryptionSettings
import com.mongodb.MongoNamespace
import com.mongodb.MongoWriteException
import com.mongodb.WriteConcern
import com.mongodb.client.test.CollectionHelper
import com.mongodb.internal.connection.TestCommandListener
import com.mongodb.reactivestreams.client.vault.ClientEncryption
import com.mongodb.reactivestreams.client.vault.ClientEncryptions
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.codecs.BsonDocumentCodec
import reactor.core.publisher.Mono

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.reactivestreams.client.Fixture.drop
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabaseName
import static com.mongodb.reactivestreams.client.Fixture.getMongoClientBuilderFromConnectionString
import static java.util.Collections.singletonMap
import static org.junit.Assume.assumeTrue
import static util.JsonPoweredTestHelper.getTestDocument

class ClientSideEncryptionBsonSizeLimitsSpecification extends FunctionalSpecification {

    private final MongoNamespace keyVaultNamespace = new MongoNamespace('test.datakeys')
    private final MongoNamespace autoEncryptingCollectionNamespace = new MongoNamespace(getDefaultDatabaseName(),
            'ClientSideEncryptionProseTestSpecification')
    private final TestCommandListener commandListener = new TestCommandListener()

    private MongoClient autoEncryptingClient
    private ClientEncryption clientEncryption
    private MongoCollection<BsonDocument> autoEncryptingDataCollection

    def setup() {
        assumeTrue(serverVersionAtLeast(4, 2))
        assumeTrue('Key vault tests disabled',
                !System.getProperty('org.mongodb.test.awsAccessKeyId', '').isEmpty())
        drop(keyVaultNamespace)
        drop(autoEncryptingCollectionNamespace)

        new CollectionHelper<>(new BsonDocumentCodec(), keyVaultNamespace).insertDocuments(
                [getTestDocument('/client-side-encryption-limits/limits-key.json')],
                WriteConcern.MAJORITY)

        def providerProperties =
                ['local': ['key': Base64.getDecoder().decode('Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN' +
                        '3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk')]
                ]

        autoEncryptingClient = MongoClients.create(getMongoClientBuilderFromConnectionString()
                .autoEncryptionSettings(AutoEncryptionSettings.builder()
                        .keyVaultNamespace(keyVaultNamespace.fullName)
                        .kmsProviders(providerProperties)
                        .schemaMap(singletonMap(autoEncryptingCollectionNamespace.fullName,
                                getTestDocument('/client-side-encryption-limits/limits-schema.json')))
                        .build())
                .addCommandListener(commandListener)
                .build())

        autoEncryptingDataCollection = autoEncryptingClient.getDatabase(autoEncryptingCollectionNamespace.databaseName)
                .getCollection(autoEncryptingCollectionNamespace.collectionName, BsonDocument)

        clientEncryption = ClientEncryptions.create(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientBuilderFromConnectionString().build())
                .keyVaultNamespace(keyVaultNamespace.fullName)
                .kmsProviders(providerProperties)
                .build())
    }

    def 'test BSON size limits'() {
        when:
        Mono.from(autoEncryptingDataCollection.insertOne(
                new BsonDocument('_id', new BsonString('over_2mib_under_16mib'))
                        .append('unencrypted', new BsonString('a' * 2097152)))).block(TIMEOUT_DURATION)

        then:
        noExceptionThrown()

        when:
        Mono.from(autoEncryptingDataCollection.insertOne(getTestDocument('/client-side-encryption-limits/limits-doc.json')
                .append('_id', new BsonString('encryption_exceeds_2mib'))
                .append('unencrypted', new BsonString('a' * (2097152 - 2000))))
        ).block(TIMEOUT_DURATION)

        then:
        noExceptionThrown()

        when:
        commandListener.reset()
        Mono.from(autoEncryptingDataCollection.insertMany(
                [
                        new BsonDocument('_id', new BsonString('over_2mib_1'))
                                .append('unencrypted', new BsonString('a' * 2097152)),
                        new BsonDocument('_id', new BsonString('over_2mib_2'))
                                .append('unencrypted', new BsonString('a' * 2097152))
                ])).block(TIMEOUT_DURATION)

        then:
        noExceptionThrown()
        countStartedEvents('insert') == 2

        when:
        commandListener.reset()
        Mono.from(autoEncryptingDataCollection.insertMany(
                [
                        getTestDocument('/client-side-encryption-limits/limits-doc.json')
                                .append('_id', new BsonString('encryption_exceeds_2mib_1'))
                                .append('unencrypted', new BsonString('a' * (2097152 - 2000))),
                        getTestDocument('/client-side-encryption-limits/limits-doc.json')
                                .append('_id', new BsonString('encryption_exceeds_2mib_2'))
                                .append('unencrypted', new BsonString('a' * (2097152 - 2000))),
                ])).block(TIMEOUT_DURATION)

        then:
        noExceptionThrown()
        countStartedEvents('insert') == 2

        when:
        Mono.from(autoEncryptingDataCollection.insertOne(
                new BsonDocument('_id', new BsonString('under_16mib'))
                        .append('unencrypted', new BsonString('a' * (16777216 - 2000))))).block(TIMEOUT_DURATION)

        then:
        noExceptionThrown()

        when:
        Mono.from(autoEncryptingDataCollection.insertOne(getTestDocument('/client-side-encryption-limits/limits-doc.json')
                .append('_id', new BsonString('encryption_exceeds_16mib'))
                .append('unencrypted', new BsonString('a' * (16777216 - 2000))))).block(TIMEOUT_DURATION)

        then:
        thrown(MongoWriteException)
    }

    private int countStartedEvents(String name) {
        int count = 0
        for (def cur : commandListener.commandStartedEvents) {
            if (cur.commandName == name) {
                count++
            }
        }
        count
    }
}
