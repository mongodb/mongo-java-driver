/*
 *
 *  * Copyright (c) 2008-2014 MongoDB, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.mongodb

import com.mongodb.client.ConcreteCodecProvider
import com.mongodb.client.MongoDatabase
import com.mongodb.client.MongoDatabaseOptions
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static com.mongodb.Fixture.getDefaultDatabaseName
import static com.mongodb.Fixture.getMongoClient

class MongoDatabaseOptionsSpecification extends Specification {

    def 'should return default value inherited from MongoClientOptions'() {
        given:
        MongoClient client = getMongoClient()
        MongoDatabase db = client.getDatabase(getDefaultDatabaseName())

        when:
        MongoDatabaseOptions options = db.getOptions()

        then:
        options.getCodecRegistry() == client.getMongoClientOptions().getCodecRegistry()
        options.getReadPreference() == client.getMongoClientOptions().getReadPreference()
        options.getWriteConcern() == client.getMongoClientOptions().getWriteConcern()
    }

    def 'should override inherited values'() {
        given:
        MongoClient client = getMongoClient()
        MongoDatabaseOptions o1 = MongoDatabaseOptions.builder()
                .writeConcern(WriteConcern.MAJORITY)
                .readPreference(ReadPreference.primaryPreferred())
                .codecRegistry(new RootCodecRegistry(Arrays.asList(new ConcreteCodecProvider())))
                .build()

        MongoDatabase db = client.getDatabase(getDefaultDatabaseName(), o1)

        when:
        MongoDatabaseOptions o2 = db.getOptions()

        then:
        o1.getCodecRegistry() == o2.getCodecRegistry()
        o1.getReadPreference() == o2.getReadPreference()
        o1.getWriteConcern() == o2.getWriteConcern()
    }


}
