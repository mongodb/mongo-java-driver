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
import com.mongodb.async.FutureResultCallback
import com.mongodb.client.options.OperationOptions
import com.mongodb.connection.Cluster
import com.mongodb.operation.GetDatabaseNamesOperation
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.ReadPreference.secondaryPreferred
import static java.util.concurrent.TimeUnit.SECONDS

class MongoClientSpecification extends Specification {

    def 'should use GetDatabaseNamesOperation for getDatabaseNames '() {
        given:
        def options = MongoClientOptions.builder().build()
        def cluster = Stub(Cluster)
        def executor = new TestOperationExecutor([['databaseName']])
        def futureResultCallback = new FutureResultCallback<List<String>>();

        when:
        new MongoClientImpl(options, cluster, executor).getDatabaseNames(futureResultCallback)

        then:
        futureResultCallback.get(10, SECONDS);
        executor.getReadOperation() instanceof GetDatabaseNamesOperation
        executor.getReadPreference() == primary()
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
                                        .writeConcern(WriteConcern.ACKNOWLEDGED)
                                        .codecRegistry(codecRegistry)
                                        .build()
        def client = new MongoClientImpl(options, Stub(Cluster), new TestOperationExecutor([]))

        when:
            def databaseOptions = customOptions ? client.getDatabase('name', customOptions).getOptions()
                                                : client.getDatabase('name').getOptions()
        then:
        databaseOptions.getReadPreference() == readPreference
        databaseOptions.getWriteConcern() == writeConcern
        databaseOptions.getCodecRegistry() == codecRegistry

        where:
        customOptions                                         | readPreference       | writeConcern              | codecRegistry
        null                                                  | secondary()          | WriteConcern.ACKNOWLEDGED | new RootCodecRegistry([])
        OperationOptions.builder().build()                    | secondary()          | WriteConcern.ACKNOWLEDGED | new RootCodecRegistry([])
        OperationOptions.builder()
                        .readPreference(secondaryPreferred())
                        .writeConcern(WriteConcern.MAJORITY)
                        .build()                              | secondaryPreferred() | WriteConcern.MAJORITY     | new RootCodecRegistry([])

    }

}
