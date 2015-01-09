/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb

import com.mongodb.client.ConcreteCodecProvider
import com.mongodb.client.options.OperationOptions
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.Fixture.getDefaultDatabaseName
import static com.mongodb.Fixture.getMongoClient
import static spock.util.matcher.HamcrestSupport.expect

class MongoCollectionConfigurationSpecification extends Specification {

    def 'should return default value inherited from MongoDatabaseOptions'() {
        given:
        def database = getMongoClient().getDatabase(getDefaultDatabaseName())

        when:
        def collection = database.getCollection('collectionName')

        then:
        expect collection.options, isTheSameAs(database.options)
    }

    def 'should override inherited values'() {
        given:
        def database = getMongoClient().getDatabase(getDefaultDatabaseName())
        def customOptions = OperationOptions.builder().build()

        when:
        def collection = database.getCollection('collectionName', customOptions)

        then:
        expect collection.options, isTheSameAs(database.options)

        when:
        customOptions = OperationOptions.builder().writeConcern(WriteConcern.MAJORITY).build()
        collection = database.getCollection('collectionName', customOptions)

        then:
        expect collection.options, isTheSameAs(database.options.withWriteConcern(WriteConcern.MAJORITY))

        when:
        customOptions = OperationOptions.builder()
                                        .writeConcern(WriteConcern.MAJORITY)
                                        .readPreference(ReadPreference.primaryPreferred())
                                        .codecRegistry(new RootCodecRegistry(Arrays.asList(new ConcreteCodecProvider())))
                                        .build()

        collection = database.getCollection('collectionName', customOptions)

        then:
        expect collection.options, isTheSameAs(customOptions)
    }

    def 'should provide new instance with updated values'() {
        given:
        def collection = getMongoClient().getDatabase(getDefaultDatabaseName()).getCollection('collectionName')

        when:
        def newCollection = collection.withReadPreference(ReadPreference.secondary())

        then:
        collection != newCollection
        expect newCollection.options, isTheSameAs(collection.getOptions().withReadPreference(ReadPreference.secondary()))

        when:
        newCollection = collection.withWriteConcern(WriteConcern.UNACKNOWLEDGED)

        then:
        collection != newCollection
        expect newCollection.options, isTheSameAs(collection.getOptions().withWriteConcern(WriteConcern.UNACKNOWLEDGED))

        when:
        def codecRegistry = new RootCodecRegistry(Arrays.asList(new ConcreteCodecProvider()))
        newCollection = collection.withCodecRegistry(codecRegistry)

        then:
        collection != newCollection
        expect newCollection.options, isTheSameAs(collection.getOptions().withCodecRegistry(codecRegistry))

        when:
        newCollection = collection.withClazz(BsonDocument)

        then:
        newCollection.clazz == BsonDocument
        collection.clazz == Document
    }

}
