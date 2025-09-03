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

package com.mongodb

import com.mongodb.connection.ClusterConnectionMode
import org.bson.UuidRepresentation
import spock.lang.Specification
import spock.lang.Unroll

import javax.net.ssl.SSLContext

import static com.mongodb.MongoCredential.createCredential
import static com.mongodb.MongoCredential.createGSSAPICredential
import static com.mongodb.MongoCredential.createMongoX509Credential
import static com.mongodb.MongoCredential.createPlainCredential
import static com.mongodb.MongoCredential.createScramSha1Credential
import static com.mongodb.ReadPreference.secondaryPreferred
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS

class MongoClientURISpecification extends Specification {

    def 'should not throw an Exception if URI contains an unknown option'() {
        when:
        new MongoClientURI('mongodb://localhost/?unknownOption=5')

        then:
        notThrown(IllegalArgumentException)
    }

    @Unroll
    def 'should parse #uri into correct components'() {
        expect:
        uri.getHosts().size() == num
        uri.getHosts() == hosts
        uri.getDatabase() == database
        uri.getCollection() == collection
        uri.getUsername() == username
        uri.getPassword() == password

        where:
        uri                                            | num | hosts              | database | collection | username | password
        new MongoClientURI('mongodb://db.example.com') | 1   | ['db.example.com'] | null     | null       | null     | null
        new MongoClientURI('mongodb://10.0.0.1')       | 1   | ['10.0.0.1']       | null     | null       | null     | null
        new MongoClientURI('mongodb://[::1]')          | 1   | ['[::1]']          | null     | null       | null     | null
        new MongoClientURI('mongodb://foo/bar')        | 1   | ['foo']            | 'bar'    | null       | null     | null
        new MongoClientURI('mongodb://10.0.0.1/bar')   | 1   | ['10.0.0.1']       | 'bar'    | null       | null     | null
        new MongoClientURI('mongodb://[::1]/bar')      | 1   | ['[::1]']          | 'bar'    | null       | null     | null
        new MongoClientURI('mongodb://localhost/' +
                           'test.my.coll')     | 1   | ['localhost']      | 'test'   | 'my.coll'  | null     | null
        new MongoClientURI('mongodb://foo/bar.goo')    | 1   | ['foo']            | 'bar'    | 'goo'      | null     | null
        new MongoClientURI('mongodb://user:pass@' +
                           'host/bar')         | 1   | ['host']           | 'bar'    | null       | 'user'   | 'pass' as char[]
        new MongoClientURI('mongodb://user:pass@' +
                           'host:27011/bar')   | 1   | ['host:27011']     | 'bar'    | null       | 'user'   | 'pass' as char[]
        new MongoClientURI('mongodb://user:pass@' +
                           '10.0.0.1:27011/bar')       | 1   | ['10.0.0.1:27011'] | 'bar'    | null       | 'user'   | 'pass' as char[]
        new MongoClientURI('mongodb://user:pass@' +
                           '[::1]:27011/bar')          | 1   | ['[::1]:27011']    | 'bar'    | null       | 'user'   | 'pass' as char[]
        new MongoClientURI('mongodb://user:pass@' +
                           'host:7,' +
                           'host2:8,' +
                           'host3:9/bar')      | 3   | ['host2:8',
                                                        'host3:9',
                                                        'host:7']         | 'bar'    | null       | 'user'   | 'pass' as char[]
        new MongoClientURI('mongodb://user:pass@' +
                           '10.0.0.1:7,' +
                           '[::1]:8,' +
                           'host3:9/bar')              | 3   | ['10.0.0.1:7',
                                                                '[::1]:8',
                                                                'host3:9']        | 'bar'    | null       | 'user'   | 'pass' as char[]
    }

    def 'should correctly parse different write concerns'() {
        expect:
        uri.getOptions().getWriteConcern() == writeConcern

        where:
        uri                                                                                  | writeConcern
        new MongoClientURI('mongodb://localhost')                                            | WriteConcern.ACKNOWLEDGED
        new MongoClientURI('mongodb://localhost/?safe=true')                                 | WriteConcern.ACKNOWLEDGED
        new MongoClientURI('mongodb://localhost/?safe=false')                                | WriteConcern.UNACKNOWLEDGED
        new MongoClientURI('mongodb://localhost/?wTimeout=5')                                | WriteConcern.ACKNOWLEDGED
                                                                                                           .withWTimeout(5, MILLISECONDS)
        new MongoClientURI('mongodb://localhost/?journal=true')                              | WriteConcern.ACKNOWLEDGED.withJournal(true)
        new MongoClientURI('mongodb://localhost/?w=2&wtimeoutMS=5&fsync=true&journal=true')  | new WriteConcern(2, 5).withJournal(true)
        new MongoClientURI('mongodb://localhost/?w=majority&wtimeoutMS=5&j=true') | new WriteConcern('majority')
                .withWTimeout(5, MILLISECONDS).withJournal(true)
    }

    @Unroll
    def 'should correctly parse legacy wtimeout write concerns'() {
        expect:
        uri.getOptions().getWriteConcern() == writeConcern

        where:
        uri                                                                                | writeConcern
        new MongoClientURI('mongodb://localhost')                                          | WriteConcern.ACKNOWLEDGED
        new MongoClientURI('mongodb://localhost/?wTimeout=5')                              | WriteConcern.ACKNOWLEDGED
                                                                                                         .withWTimeout(5, MILLISECONDS)
        new MongoClientURI('mongodb://localhost/?w=2&wtimeout=5&j=true')        | new WriteConcern(2, 5).withJournal(true)
        new MongoClientURI('mongodb://localhost/?w=majority&wtimeout=5&j=true') | new WriteConcern('majority')
                .withWTimeout(5, MILLISECONDS).withJournal(true)
        new MongoClientURI('mongodb://localhost/?wTimeout=1&wtimeoutMS=5')                 | WriteConcern.ACKNOWLEDGED
                                                                                                         .withWTimeout(5, MILLISECONDS)
    }

    def 'should correctly parse URI options for #type'() {
        given:
        def uri = new MongoClientURI('mongodb://localhost/?minPoolSize=5&maxPoolSize=10&waitQueueTimeoutMS=150&'
                + 'maxIdleTimeMS=200&maxLifeTimeMS=300&maxConnecting=1&replicaSet=test&'
                + 'connectTimeoutMS=2500&socketTimeoutMS=5500&'
                + 'safe=false&w=1&wtimeout=2500&ssl=true&readPreference=secondary&'
                + 'sslInvalidHostNameAllowed=true&'
                + 'serverSelectionTimeoutMS=25000&'
                + 'localThresholdMS=30&'
                + 'heartbeatFrequencyMS=20000&'
                + 'retryWrites=true&'
                + 'retryReads=true&'
                + 'uuidRepresentation=csharpLegacy&'
                + 'appName=app1&'
                + 'timeoutMS=10000')

        when:
        def options = uri.getOptions()

        then:
        options.getWriteConcern() == new WriteConcern(1, 2500)
        options.getReadPreference() == ReadPreference.secondary()
        options.getConnectionsPerHost() == 10
        options.getMinConnectionsPerHost() == 5
        options.getMaxWaitTime() == 150
        options.getMaxConnectionIdleTime() == 200
        options.getMaxConnectionLifeTime() == 300
        options.getMaxConnecting() == 1
        options.getTimeout() == 10_000
        options.getSocketTimeout() == 5500
        options.getConnectTimeout() == 2500
        options.getRequiredReplicaSetName() == 'test'
        options.isSslEnabled()
        options.isSslInvalidHostNameAllowed()
        options.getServerSelectionTimeout() == 25000
        options.getLocalThreshold() == 30
        options.getHeartbeatFrequency() == 20000
        options.getRetryWrites()
        options.getRetryReads()
        options.getUuidRepresentation() == UuidRepresentation.C_SHARP_LEGACY
        options.getApplicationName() == 'app1'
    }

    def 'should have correct defaults for options'() {
        when:
        MongoClientOptions options = new MongoClientURI('mongodb://localhost').getOptions()

        then:
        options.getConnectionsPerHost() == 100
        options.getMaxConnecting() == 2
        options.getTimeout() == null
        options.getMaxWaitTime() == 120000
        options.getConnectTimeout() == 10000
        options.getSocketTimeout() == 0
        options.getReadPreference() == ReadPreference.primary()
        options.getRequiredReplicaSetName() == null
        !options.isSslEnabled()
        options.getRetryWrites()
        options.getRetryReads()
        options.getUuidRepresentation() == UuidRepresentation.UNSPECIFIED
    }

    def 'should apply default uri to options'() {
        given:
        def optionsBuilder = MongoClientOptions.builder()
                .applicationName('appName')
                .readPreference(ReadPreference.secondary())
                .retryWrites(true)
                .retryReads(true)
                .writeConcern(WriteConcern.JOURNALED)
                .minConnectionsPerHost(30)
                .connectionsPerHost(500)
                .timeout(10_000)
                .connectTimeout(100)
                .socketTimeout(700)
                .serverSelectionTimeout(150)
                .maxWaitTime(200)
                .maxConnectionIdleTime(300)
                .maxConnectionLifeTime(400)
                .maxConnecting(1)
                .sslEnabled(true)
                .sslInvalidHostNameAllowed(true)
                .sslContext(SSLContext.getDefault())
                .heartbeatFrequency(5)
                .minHeartbeatFrequency(11)
                .heartbeatConnectTimeout(15)
                .heartbeatSocketTimeout(20)
                .localThreshold(25)
                .requiredReplicaSetName('test')
                .compressorList([MongoCompressor.createZlibCompressor()])
                .uuidRepresentation(UuidRepresentation.C_SHARP_LEGACY)

        when:
        def options = new MongoClientURI('mongodb://localhost', optionsBuilder).getOptions()

        then:
        options.getApplicationName() == 'appName'
        options.getReadPreference() == ReadPreference.secondary()
        options.getWriteConcern() == WriteConcern.JOURNALED
        options.getRetryWrites()
        options.getRetryReads()
        options.getTimeout() == 10_000
        options.getServerSelectionTimeout() == 150
        options.getMaxWaitTime() == 200
        options.getMaxConnectionIdleTime() == 300
        options.getMaxConnectionLifeTime() == 400
        options.getMaxConnecting() == 1
        options.getMinConnectionsPerHost() == 30
        options.getConnectionsPerHost() == 500
        options.getConnectTimeout() == 100
        options.getSocketTimeout() == 700
        options.isSslEnabled()
        options.isSslInvalidHostNameAllowed()
        options.getHeartbeatFrequency() == 5
        options.getMinHeartbeatFrequency() == 11
        options.getHeartbeatConnectTimeout() == 15
        options.getHeartbeatSocketTimeout() == 20
        options.getLocalThreshold() == 25
        options.getRequiredReplicaSetName() == 'test'
        options.asMongoClientSettings(null, null, ClusterConnectionMode.SINGLE, null)
                .getServerSettings().getHeartbeatFrequency(MILLISECONDS) == 5
        options.asMongoClientSettings(null, null, ClusterConnectionMode.SINGLE, null)
                .getServerSettings().getMinHeartbeatFrequency(MILLISECONDS) == 11
        options.compressorList == [MongoCompressor.createZlibCompressor()]
        options.getUuidRepresentation() == UuidRepresentation.C_SHARP_LEGACY
    }

    @Unroll
    def 'should support all credential types'() {
        expect:
        uri.credentials == credentialList

        where:
        uri                                                   | credentialList
        new MongoClientURI('mongodb://jeff:123@localhost')    | createCredential('jeff', 'admin', '123'.toCharArray())
        new MongoClientURI('mongodb://jeff:123@localhost/?' +
                           'authMechanism=MONGODB-CR')        | createCredential('jeff', 'admin', '123'.toCharArray())
        new MongoClientURI('mongodb://jeff:123@localhost/?' +
                           'authMechanism=MONGODB-CR' +
                           '&authSource=test')                | createCredential('jeff', 'test', '123'.toCharArray())
        new MongoClientURI('mongodb://jeff:123@localhost/?' +
                           'authMechanism=SCRAM-SHA-1')       | createScramSha1Credential('jeff', 'admin', '123'.toCharArray())
        new MongoClientURI('mongodb://jeff:123@localhost/?' +
                           'authMechanism=SCRAM-SHA-1' +
                           '&authSource=test')                | createScramSha1Credential('jeff', 'test', '123'.toCharArray())
        new MongoClientURI('mongodb://jeff@localhost/?' +
                           'authMechanism=GSSAPI')            | createGSSAPICredential('jeff')
        new MongoClientURI('mongodb://jeff:123@localhost/?' +
                           'authMechanism=PLAIN')             | createPlainCredential('jeff', '$external', '123'.toCharArray())
        new MongoClientURI('mongodb://jeff@localhost/?' +
                           'authMechanism=MONGODB-X509')      | createMongoX509Credential('jeff')
        new MongoClientURI('mongodb://jeff@localhost/?' +
                           'authMechanism=GSSAPI' +
                           '&gssapiServiceName=foo')          | createGSSAPICredential('jeff').withMechanismProperty('SERVICE_NAME', 'foo')
        new MongoClientURI('mongodb://jeff:123@localhost/?' +
                           'authMechanism=SCRAM-SHA-1')       | createScramSha1Credential('jeff', 'admin', '123'.toCharArray())
    }

    @Unroll
    def 'should correct parse read preference for #readPreference'() {
        expect:
        uri.getOptions().getReadPreference() == readPreference

        where:
        uri                                                      | readPreference
        new MongoClientURI('mongodb://localhost/' +
                           '?readPreference=secondaryPreferred') | secondaryPreferred()
        new MongoClientURI('mongodb://localhost/' +
                           '?readPreference=secondaryPreferred' +
                           '&readPreferenceTags=dc:ny,rack:1' +
                           '&readPreferenceTags=dc:ny' +
                           '&readPreferenceTags=')               | secondaryPreferred([new TagSet(asList(new Tag('dc', 'ny'),
                                                                                                         new Tag('rack', '1'))),
                                                                                       new TagSet(asList(new Tag('dc', 'ny'))),
                                                                                       new TagSet()])
    }

    def 'should apply SRV parameters'() {
        when:
        def uri = new MongoClientURI('mongodb+srv://test3.test.build.10gen.cc/?srvMaxHosts=4&srvServiceName=test')

        then:
        uri.getSrvMaxHosts() == 4
        uri.getSrvServiceName() == 'test'

        when:
        def options = uri.getOptions()

        then:
        options.getSrvMaxHosts() == 4
        options.getSrvServiceName() == 'test'
    }

    def 'should respect MongoClientOptions builder'() {
        given:
        def uri = new MongoClientURI('mongodb://localhost/', MongoClientOptions.builder().connectionsPerHost(200))

        when:
        def options = uri.getOptions()

        then:
        options.getConnectionsPerHost() == 200
    }

    def 'should override MongoClientOptions builder'() {
        given:
        def uri = new MongoClientURI('mongodb://localhost/?maxPoolSize=250', MongoClientOptions.builder().connectionsPerHost(200))

        when:
        def options = uri.getOptions()

        then:
        options.getConnectionsPerHost() == 250
    }

    def 'should be equal to another MongoClientURI with the same string values'() {
        expect:
        uri1 == uri2
        uri1.hashCode() == uri2.hashCode()

        where:
        uri1                                                        | uri2
        new MongoClientURI('mongodb://user:pass@host1:1/')          | new MongoClientURI('mongodb://user:pass@host1:1/')
        new MongoClientURI('mongodb://user:pass@host1:1,host2:2,'
                                     + 'host3:3/bar')               | new MongoClientURI('mongodb://user:pass@host3:3,host1:1,'
                                                                                                   + 'host2:2/bar')
        new MongoClientURI('mongodb://localhost/'
                         + '?readPreference=secondaryPreferred'
                         + '&readPreferenceTags=dc:ny,rack:1'
                         + '&readPreferenceTags=dc:ny'
                         + '&readPreferenceTags=')                  | new MongoClientURI('mongodb://localhost/'
                                                                                       + '?readPreference=secondaryPreferred'
                                                                                       + '&readPreferenceTags=dc:ny, rack:1'
                                                                                       + '&readPreferenceTags=dc:ny'
                                                                                       + '&readPreferenceTags=')
        new MongoClientURI('mongodb://localhost/?readPreference='
                         + 'secondaryPreferred')                    | new MongoClientURI('mongodb://localhost/?readPreference='
                                                                                       + 'secondaryPreferred')
        new MongoClientURI('mongodb://ross:123@localhost/?'
                         + 'authMechanism=SCRAM-SHA-1')             | new MongoClientURI('mongodb://ross:123@localhost/?'
                                                                                       + 'authMechanism=SCRAM-SHA-1')
        new MongoClientURI('mongodb://localhost/db.coll'
                         + '?minPoolSize=5;'
                         + 'maxPoolSize=10;'
                         + 'waitQueueTimeoutMS=150;'
                         + 'maxIdleTimeMS=200;'
                         + 'maxLifeTimeMS=300;replicaSet=test;'
                         + 'maxConnecting=1;'
                         + 'connectTimeoutMS=2500;'
                         + 'socketTimeoutMS=5500;'
                         + 'safe=false;w=1;wtimeout=2500;'
                         + 'fsync=true;readPreference=primary;'
                         + 'ssl=true')                               |  new MongoClientURI('mongodb://localhost/db.coll?minPoolSize=5;'
                                                                                         + 'maxPoolSize=10;'
                                                                                         + 'waitQueueTimeoutMS=150;'
                                                                                         + 'maxIdleTimeMS=200&maxLifeTimeMS=300;'
                                                                                         + 'maxConnecting=1;'
                                                                                         + '&replicaSet=test;connectTimeoutMS=2500;'
                                                                                         + 'socketTimeoutMS=5500&safe=false&w=1;'
                                                                                         + 'wtimeout=2500;fsync=true'
                                                                                         + '&readPreference=primary;ssl=true')
    }

    def 'should be not equal to another MongoClientURI with the different string values'() {
        expect:
        uri1 != uri2
        uri1.hashCode() != uri2.hashCode()

        where:
        uri1                                                        | uri2
        new MongoClientURI('mongodb://user:pass@host1:1/')          | new MongoClientURI('mongodb://user:pass@host1:2/')
        new MongoClientURI('mongodb://user:pass@host1:1,host2:2,'
                                   + 'host3:3/bar')                 | new MongoClientURI('mongodb://user:pass@host1:1,host2:2,'
                                                                                       + 'host4:4/bar')
        new MongoClientURI('mongodb://localhost/?readPreference='
                        + 'secondaryPreferred')                     | new MongoClientURI('mongodb://localhost/?readPreference='
                                                                                      + 'secondary')
        new MongoClientURI('mongodb://localhost/'
                         + '?readPreference=secondaryPreferred'
                         + '&readPreferenceTags=dc:ny,rack:1'
                         + '&readPreferenceTags=dc:ny'
                         + '&readPreferenceTags='
                         + '&maxConnecting=1')                  | new MongoClientURI('mongodb://localhost/'
                                                                                       + '?readPreference=secondaryPreferred'
                                                                                       + '&readPreferenceTags=dc:ny'
                                                                                       + '&readPreferenceTags=dc:ny, rack:1'
                                                                                       + '&readPreferenceTags='
                                                                                       + '&maxConnecting=2')
        new MongoClientURI('mongodb://ross:123@localhost/?'
                         + 'authMechanism=SCRAM-SHA-1')            | new MongoClientURI('mongodb://ross:123@localhost/?'
                                                                                       + 'authMechanism=GSSAPI')
    }

    def 'should be equal to another MongoClientURI with options'() {
        when:
        MongoClientURI uri1 = new MongoClientURI('mongodb://user:pass@host1:1,host2:2,host3:3/bar?'
                                                        + 'maxPoolSize=10;waitQueueTimeoutMS=150;'
                                                        + 'minPoolSize=7;maxIdleTimeMS=1000;maxLifeTimeMS=2000;maxConnecting=1;'
                                                        + 'replicaSet=test;'
                                                        + 'connectTimeoutMS=2500;socketTimeoutMS=5500;autoConnectRetry=true;'
                                                        + 'readPreference=secondaryPreferred;safe=false;w=1;wtimeout=2600')

        MongoClientOptions.Builder builder = MongoClientOptions.builder()
                                                               .connectionsPerHost(10)
                                                               .maxWaitTime(150)
                                                               .minConnectionsPerHost(7)
                                                               .maxConnectionIdleTime(1000)
                                                               .maxConnectionLifeTime(2000)
                                                               .maxConnecting(1)
                                                               .requiredReplicaSetName('test')
                                                               .connectTimeout(2500)
                                                               .socketTimeout(5500)
                                                               .readPreference(secondaryPreferred())
                                                               .writeConcern(new WriteConcern(1, 2600))

        MongoClientOptions options = builder.build()

        then:
        uri1.getOptions() == options

        when:
        MongoClientURI uri2 = new MongoClientURI('mongodb://user:pass@host3:3,host1:1,host2:2/bar?', builder)

        then:
        uri1 == uri2
        uri1.hashCode() == uri2.hashCode()
    }
}
