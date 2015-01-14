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

package com.mongodb.async.client

import com.mongodb.WriteConcern
import com.mongodb.connection.Cluster
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static spock.util.matcher.HamcrestSupport.expect

class MongoClientSpecification extends Specification {

    def 'should use ListCollectionsOperation correctly'() {
        given:
        def options = MongoClientOptions.builder().build()
        def cluster = Stub(Cluster)
        def executor = new TestOperationExecutor([null, null, null])
        def client = new MongoClientImpl(options, cluster, executor)
        def codecRegistry = client.getDefaultCodecRegistry()

        when:
        def listDatabasesFluent = client.listDatabases()

        then:
        expect listDatabasesFluent, isTheSameAs(new ListDatabasesFluentImpl<Document>(Document, codecRegistry, primary(),
                executor))

        when:
        listDatabasesFluent = client.listDatabases(BsonDocument)

        then:
        expect listDatabasesFluent, isTheSameAs(new ListDatabasesFluentImpl<BsonDocument>(BsonDocument, codecRegistry, primary(),
                executor))
    }

    def 'should provide the same options'() {
        given:
        def options = MongoClientOptions.builder().build()

        when:
        def clientOptions = new MongoClientImpl(options, Stub(Cluster), new TestOperationExecutor([])).getOptions()

        then:
        options == clientOptions
    }

    def 'should pass the correct options to getDatabase'() {
        given:
        def options = MongoClientOptions.builder()
                                        .readPreference(secondary())
                                        .writeConcern(WriteConcern.MAJORITY)
                                        .codecRegistry(new RootCodecRegistry([]))
                                        .build()
        def client = new MongoClientImpl(options, Stub(Cluster), new TestOperationExecutor([]))

        when:
        def database = client.getDatabase('name');

        then:
        expect database, isTheSameAs(expectedDatabase)

        where:
        expectedDatabase << new MongoDatabaseImpl('name', new RootCodecRegistry([]), secondary(), WriteConcern.MAJORITY,
                new TestOperationExecutor([]))
    }

}
