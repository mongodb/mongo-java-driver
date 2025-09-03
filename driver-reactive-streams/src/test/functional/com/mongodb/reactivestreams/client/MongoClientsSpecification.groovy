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

import com.mongodb.MongoClientSettings
import com.mongodb.MongoCompressor
import com.mongodb.MongoCredential
import com.mongodb.ReadConcern
import com.mongodb.ServerAddress
import com.mongodb.WriteConcern
import com.mongodb.connection.TransportSettings
import com.mongodb.reactivestreams.client.internal.MongoClientImpl
import org.bson.Document
import reactor.core.publisher.Mono
import spock.lang.IgnoreIf
import spock.lang.Unroll

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION
import static com.mongodb.ClusterFixture.connectionString
import static com.mongodb.ClusterFixture.getCredential
import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.ClusterFixture.getServerApi
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondaryPreferred
import static java.util.concurrent.TimeUnit.MILLISECONDS

@IgnoreIf({ getServerApi() != null })
class MongoClientsSpecification extends FunctionalSpecification {

    def 'should connect'() {
        given:
        def connectionString = 'mongodb://'
        if (!getCredential() == null) {
           connectionString += (getCredential().getUserName() + ':' + String.valueOf(getCredential().getPassword()) + '@')
        }
        connectionString += getConnectionString().getHosts()[0] + '/?'
        connectionString += 'ssl=' + getSslSettings().isEnabled() + '&'
        connectionString += 'sslInvalidHostNameAllowed=' + getSslSettings().isInvalidHostNameAllowed()

        when:
        def client = MongoClients.create(connectionString)
        Mono.from(client.getDatabase('admin').runCommand(new Document('ping', 1))).block(TIMEOUT_DURATION)

        then:
        noExceptionThrown()

        cleanup:
        client?.close()
    }

    def 'should apply connection string to cluster settings'() {
        when:
        def client = MongoClients.create('mongodb://localhost,localhost:27018/') as MongoClientImpl

        then:
        client.settings.clusterSettings.hosts == [new ServerAddress('localhost'), new ServerAddress('localhost:27018')]

        cleanup:
        client?.close()
    }

    def 'should apply connection string to credential list'() {
        when:
        def client = MongoClients.create('mongodb://u:p@localhost/') as MongoClientImpl

        then:
        client.settings.credential == MongoCredential.createCredential('u', 'admin', 'p'.toCharArray())

        cleanup:
        client?.close()
    }

    def 'should apply connection string to server settings'() {
        when:
        def client = MongoClients.create('mongodb://localhost/?heartbeatFrequencyMS=50') as MongoClientImpl

        then:
        client.settings.serverSettings.getHeartbeatFrequency(MILLISECONDS) == 50

        cleanup:
        client?.close()
    }

    def 'should apply connection string to connection pool settings'() {
        when:
        def client = MongoClients.create('mongodb://localhost/?maxIdleTimeMS=200&maxLifeTimeMS=300') as MongoClientImpl

        then:
        client.settings.connectionPoolSettings.getMaxConnectionIdleTime(MILLISECONDS) == 200
        client.settings.connectionPoolSettings.getMaxConnectionLifeTime(MILLISECONDS) == 300

        cleanup:
        client?.close()
    }

    def 'should apply connection string to ssl settings'() {
        when:
        def client = MongoClients.create('mongodb://localhost/?ssl=true&sslInvalidHostNameAllowed=true') as MongoClientImpl

        then:
        client.settings.sslSettings.enabled
        client.settings.sslSettings.invalidHostNameAllowed

        cleanup:
        client?.close()
    }

    def 'should apply connection string to socket settings'() {
        when:
        def client = MongoClients.create('mongodb://localhost/?connectTimeoutMS=300') as MongoClientImpl

        then:
        client.settings.socketSettings.getConnectTimeout(MILLISECONDS) == 300

        cleanup:
        client?.close()
    }

    @Unroll
    def 'should apply read preference from connection string to settings'() {
        when:
        def client = MongoClients.create(uri) as MongoClientImpl

        then:
        client.settings.getReadPreference() == readPreference

        cleanup:
        client?.close()

        where:
        uri                                                              | readPreference
        'mongodb://localhost/'                                           | primary()
        'mongodb://localhost/?readPreference=secondaryPreferred'         | secondaryPreferred()
    }

    @Unroll
    def 'should apply read concern from connection string to settings'() {
        when:
        def client = MongoClients.create(uri) as MongoClientImpl

        then:
        client.settings.getReadConcern() == readConcern

        cleanup:
        client?.close()

        where:
        uri                                               | readConcern
        'mongodb://localhost/'                            | ReadConcern.DEFAULT
        'mongodb://localhost/?readConcernLevel=local'     | ReadConcern.LOCAL
    }

    @Unroll
    def 'should apply write concern from connection string to settings'() {
        when:
        def client = MongoClients.create(uri) as MongoClientImpl

        then:
        client.settings.getWriteConcern() == writeConcern

        cleanup:
        client?.close()

        where:
        uri                               | writeConcern
        'mongodb://localhost'             | WriteConcern.ACKNOWLEDGED
        'mongodb://localhost/?w=majority' | WriteConcern.MAJORITY
    }

    @Unroll
    def 'should apply application name from connection string to settings'() {
        when:
        def client = MongoClients.create(uri) as MongoClientImpl

        then:
        client.settings.getApplicationName() == applicationName

        cleanup:
        client?.close()

        where:
        uri                                 | applicationName
        'mongodb://localhost'               | null
        'mongodb://localhost/?appname=app1' | 'app1'
    }

    @Unroll
    def 'should apply compressors from connection string to settings'() {
        when:
        def client = MongoClients.create(uri) as MongoClientImpl

        then:
        client.settings.getCompressorList() == compressorList

        cleanup:
        client?.close()

        where:
        uri                                         | compressorList
        'mongodb://localhost'                       | []
        'mongodb://localhost/?compressors=zlib'     | [MongoCompressor.createZlibCompressor()]
        'mongodb://localhost/?compressors=zstd'     | [MongoCompressor.createZstdCompressor()]
    }

    def 'should create client with transport settings'() {
        given:
        def nettySettings = TransportSettings.nettyBuilder().build()
        def settings = MongoClientSettings.builder()
                .transportSettings(nettySettings)
                .build()

        when:
        def client = MongoClients.create(settings)

        then:
        true

        cleanup:
        client?.close()
    }
}
