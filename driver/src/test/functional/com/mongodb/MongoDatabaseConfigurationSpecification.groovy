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

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.Fixture.getDefaultDatabaseName
import static com.mongodb.Fixture.getMongoClient
import static spock.util.matcher.HamcrestSupport.expect

class MongoDatabaseConfigurationSpecification extends Specification {

    def 'should return default value inherited from MongoClientOptions'() {
        given:
        def client = getMongoClient()
        def clientOptions = client.getMongoClientOptions()
        def defaultOptions = OperationOptions.builder()
                .codecRegistry(clientOptions.getCodecRegistry())
                .readPreference(clientOptions.getReadPreference())
                .writeConcern(clientOptions.getWriteConcern())
                .build()

        when:
        def database = client.getDatabase(getDefaultDatabaseName())

        then:
        expect database.options, isTheSameAs(defaultOptions)
    }

    def 'should override inherited values'() {
        given:
        def client = getMongoClient()
        def clientOptions = client.getMongoClientOptions()
        def defaultOptions = OperationOptions.builder()
                .codecRegistry(clientOptions.getCodecRegistry())
                .readPreference(clientOptions.getReadPreference())
                .writeConcern(clientOptions.getWriteConcern())
                .build()

        when:
        def customOptions = OperationOptions.builder().build()
        def database = client.getDatabase(getDefaultDatabaseName(), customOptions)

        then:
        expect database.options, isTheSameAs(defaultOptions)

        when:
        customOptions = OperationOptions.builder().writeConcern(WriteConcern.MAJORITY).build()
        database = client.getDatabase(getDefaultDatabaseName(), customOptions)

        then:
        expect database.options, isTheSameAs(defaultOptions.withWriteConcern(WriteConcern.MAJORITY))

        when:
        customOptions = OperationOptions.builder()
                .writeConcern(WriteConcern.MAJORITY)
                .readPreference(ReadPreference.primaryPreferred())
                .codecRegistry(new RootCodecRegistry(Arrays.asList(new ConcreteCodecProvider())))
                .build()
        database = client.getDatabase(getDefaultDatabaseName(), customOptions)

        then:
        expect database.options, isTheSameAs(customOptions)
    }

}
