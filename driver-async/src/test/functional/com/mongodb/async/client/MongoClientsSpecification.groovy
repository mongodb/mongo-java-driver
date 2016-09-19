/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.async.client

import com.mongodb.MongoCredential
import com.mongodb.ReadConcern
import com.mongodb.ServerAddress
import com.mongodb.WriteConcern
import com.mongodb.client.MongoDriverInformation
import com.mongodb.connection.AsynchronousSocketChannelStreamFactoryFactory
import com.mongodb.connection.netty.NettyStreamFactoryFactory
import org.bson.Document
import spock.lang.IgnoreIf
import spock.lang.Unroll

import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.ClusterFixture.isStandalone
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondaryPreferred
import static com.mongodb.async.client.Fixture.getMongoClientBuilderFromConnectionString
import static com.mongodb.async.client.TestHelper.run
import static java.util.concurrent.TimeUnit.MILLISECONDS

class MongoClientsSpecification extends FunctionalSpecification {

    def 'should apply connection string to cluster settings'() {
        when:
        def client = MongoClients.create('mongodb://localhost,localhost:27018/')

        then:
        client.settings.clusterSettings.hosts == [new ServerAddress('localhost'), new ServerAddress('localhost:27018')]

        cleanup:
        client?.close()
    }

    def 'should apply connection string to credential list'() {
        when:
        def client = MongoClients.create('mongodb://u:p@localhost/')

        then:
        client.settings.credentialList == [MongoCredential.createCredential('u', 'admin', 'p'.toCharArray())]

        cleanup:
        client?.close()
    }

    def 'should apply connection string to server settings'() {
        when:
        def client = MongoClients.create('mongodb://localhost/?heartbeatFrequencyMS=50')

        then:
        client.settings.serverSettings.getHeartbeatFrequency(MILLISECONDS) == 50

        cleanup:
        client?.close()
    }

    def 'should apply connection string to connection pool settings'() {
        when:
        def client = MongoClients.create('mongodb://localhost/?maxIdleTimeMS=200&maxLifeTimeMS=300')

        then:
        client.settings.connectionPoolSettings.getMaxConnectionIdleTime(MILLISECONDS) == 200
        client.settings.connectionPoolSettings.getMaxConnectionLifeTime(MILLISECONDS) == 300

        cleanup:
        client?.close()
    }

    def 'should apply connection string to ssl settings'() {
        when:
        def client = MongoClients.create('mongodb://localhost/?ssl=true&sslInvalidHostNameAllowed=true&streamType=netty')

        then:
        client.settings.sslSettings.enabled
        client.settings.sslSettings.invalidHostNameAllowed

        cleanup:
        client?.close()
    }

    def 'should apply connection string to netty stream type'() {
        when:
        def client = MongoClients.create('mongodb://localhost/?streamType=Netty')

        then:
        client.settings.streamFactoryFactory instanceof NettyStreamFactoryFactory

        cleanup:
        client?.close()
    }

    @IgnoreIf({ javaVersion < 1.7 || getSslSettings().isEnabled() })
    def 'should apply connection string to nio2 stream type'() {
        when:
        def client = MongoClients.create('mongodb://localhost/?streamType=NIO2')

        then:
        client.settings.streamFactoryFactory instanceof AsynchronousSocketChannelStreamFactoryFactory

        cleanup:
        client?.close()
    }

    def 'should apply connection string to socket settings'() {
        when:
        def client = MongoClients.create('mongodb://localhost/?connectTimeoutMS=300')

        then:
        client.settings.socketSettings.getConnectTimeout(MILLISECONDS) == 300

        cleanup:
        client?.close()
    }

    @Unroll
    def 'should apply read preference from connection string to settings'() {
        when:
        def client = MongoClients.create(uri)

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
        def client = MongoClients.create(uri)

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
        def client = MongoClients.create(uri)

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
        def client = MongoClients.create(uri)

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
    def 'should respect the streamType over the system properties'() {
        given:
        def asyncType = System.getProperty('org.mongodb.async.type', null)
        System.setProperty('org.mongodb.async.type', systemType)

        when:
        def client = MongoClients.create(uri)

        then:
        client.settings.getStreamFactoryFactory().getClass() == streamFactoryFactoryClass

        cleanup:
        client?.close()
        if (asyncType != null) {
            System.setProperty('org.mongodb.async.type', asyncType)
        }

        where:
        uri                                     |  systemType | streamFactoryFactoryClass
        'mongodb://localhost/?streamType=nio2'  | 'netty'     | AsynchronousSocketChannelStreamFactoryFactory
        'mongodb://localhost/?streamType=netty' | 'nio2'      | NettyStreamFactoryFactory
    }

    @IgnoreIf({ !serverVersionAtLeast([3, 3, 9]) || !isStandalone() })
    def 'application name should appear in the system.profile collection'() {
        given:
        def appName = 'appName1'
        def driverInfo = MongoDriverInformation.builder().driverName('myDriver').driverVersion('42').build()
        def client = MongoClients.create(getMongoClientBuilderFromConnectionString().applicationName(appName).build(), driverInfo)
        def database = client.getDatabase(getDatabaseName())
        def collection = database.getCollection(getCollectionName())

        def profileCollection = database.getCollection('system.profile')
        run(profileCollection.&drop)
        run(database.&runCommand, new Document('profile', 2))

        when:
        run(collection.&count)

        then:
        Document profileDocument = run(profileCollection.find().&first)
        profileDocument.get('appName') == appName

        cleanup:
        if (database != null) {
            run(database.&runCommand, new Document('profile', 0))
        }
        if (profileCollection != null) {
            run(profileCollection.&drop)
        }
        client?.close()
    }
}
