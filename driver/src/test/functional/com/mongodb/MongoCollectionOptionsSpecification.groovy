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
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static com.mongodb.Fixture.getDefaultDatabaseName
import static com.mongodb.Fixture.getMongoClient

class MongoCollectionOptionsSpecification extends Specification {

    def 'should return default value inherited from MongoDatabaseOptions'() {
        given:
        def database = getMongoClient().getDatabase(getDefaultDatabaseName())
        def collection = database.getCollection('test')

        when:
        def options = collection.getOptions()

        then:
        options.getCodecRegistry() == database.getOptions().getCodecRegistry()
        options.getReadPreference() == database.getOptions().getReadPreference()
        options.getWriteConcern() == database.getOptions().getWriteConcern()
    }

    def 'should override inherited values'() {
        given:
        def database = getMongoClient().getDatabase(getDefaultDatabaseName())
        def options
        def customOptions

        when:
        customOptions = OperationOptions.builder().build()
        options = database.getCollection(getDefaultDatabaseName(), customOptions).getOptions()

        then:
        options.getCodecRegistry() == database.getOptions().getCodecRegistry()
        options.getReadPreference() == database.getOptions().getReadPreference()
        options.getWriteConcern() == database.getOptions().getWriteConcern()

        when:
        customOptions = OperationOptions.builder().writeConcern(WriteConcern.MAJORITY).build()
        options = database.getCollection(getDefaultDatabaseName(), customOptions).getOptions()

        then:
        options.getCodecRegistry() == database.getOptions().getCodecRegistry()
        options.getReadPreference() == database.getOptions().getReadPreference()
        options.getWriteConcern() == customOptions.getWriteConcern()

        when:
        customOptions = OperationOptions.builder()
                                        .writeConcern(WriteConcern.MAJORITY)
                                        .readPreference(ReadPreference.primaryPreferred())
                                        .codecRegistry(new RootCodecRegistry(Arrays.asList(new ConcreteCodecProvider())))
                                        .build()

        options = database.getCollection(getDefaultDatabaseName(), customOptions).getOptions()

        then:
        options.getCodecRegistry() == customOptions.getCodecRegistry()
        options.getReadPreference() == customOptions.getReadPreference()
        options.getWriteConcern() == customOptions.getWriteConcern()
    }

}
