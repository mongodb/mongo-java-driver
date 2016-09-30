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

import spock.lang.IgnoreIf
import spock.lang.Specification
import spock.lang.Unroll

import static com.mongodb.ClusterFixture.isNotAtLeastJava7
import static com.mongodb.MongoCredential.createCredential
import static com.mongodb.MongoCredential.createGSSAPICredential
import static com.mongodb.MongoCredential.createMongoCRCredential
import static com.mongodb.MongoCredential.createMongoX509Credential
import static com.mongodb.MongoCredential.createPlainCredential
import static com.mongodb.MongoCredential.createScramSha1Credential
import static com.mongodb.ReadPreference.secondaryPreferred
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS

class MongoClientURISpecification extends Specification {

    def 'should throw Exception if URI does not have a trailing slash'() {
        when:
        new MongoClientURI('mongodb://localhost?wtimeoutMS=5');

        then:
        thrown(IllegalArgumentException)
    }

    def 'should not throw an Exception if URI contains an unknown option'() {
        when:
        new MongoClientURI('mongodb://localhost/?unknownOption=5');

        then:
        notThrown(IllegalArgumentException)
    }

    @Unroll
    def 'should parse #uri into correct components'() {
        expect:
        uri.getHosts().size() == num;
        uri.getHosts() == hosts;
        uri.getDatabase() == database;
        uri.getCollection() == collection;
        uri.getUsername() == username;
        uri.getPassword() == password;

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
        uri.getOptions().getWriteConcern() == writeConcern;

        where:
        uri                                                                                  | writeConcern
        new MongoClientURI('mongodb://localhost')                                            | WriteConcern.ACKNOWLEDGED
        new MongoClientURI('mongodb://localhost/?safe=true')                                 | WriteConcern.ACKNOWLEDGED
        new MongoClientURI('mongodb://localhost/?safe=false')                                | WriteConcern.UNACKNOWLEDGED
        new MongoClientURI('mongodb://localhost/?wTimeout=5')                                | WriteConcern.ACKNOWLEDGED
                                                                                                           .withWTimeout(5, MILLISECONDS)
        new MongoClientURI('mongodb://localhost/?fsync=true')                                | WriteConcern.ACKNOWLEDGED.withFsync(true)
        new MongoClientURI('mongodb://localhost/?journal=true')                              | WriteConcern.ACKNOWLEDGED.withJournal(true)
        new MongoClientURI('mongodb://localhost/?w=2&wtimeoutMS=5&fsync=true&journal=true')  | new WriteConcern(2, 5, true, true)
        new MongoClientURI('mongodb://localhost/?w=majority&wtimeoutMS=5&fsync=true&j=true') | new WriteConcern('majority', 5, true, true)
    }

    def 'should correctly parse legacy wtimeout write concerns'() {
        expect:
        uri.getOptions().getWriteConcern() == writeConcern;

        where:
        uri                                                                                | writeConcern
        new MongoClientURI('mongodb://localhost')                                          | WriteConcern.ACKNOWLEDGED
        new MongoClientURI('mongodb://localhost/?wTimeout=5')                              | WriteConcern.ACKNOWLEDGED
                                                                                                         .withWTimeout(5, MILLISECONDS)
        new MongoClientURI('mongodb://localhost/?w=2&wtimeout=5&fsync=true&j=true')        | new WriteConcern(2, 5, true, true)
        new MongoClientURI('mongodb://localhost/?w=majority&wtimeout=5&fsync=true&j=true') | new WriteConcern('majority', 5, true, true)
        new MongoClientURI('mongodb://localhost/?wTimeout=1&wtimeoutMS=5')                 | WriteConcern.ACKNOWLEDGED
                                                                                                         .withWTimeout(5, MILLISECONDS)
    }

    @IgnoreIf({ isNotAtLeastJava7() })
    def 'should correctly parse URI options for #type'() {
        given:
        def uri = new MongoClientURI('mongodb://localhost/?minPoolSize=5&maxPoolSize=10&waitQueueMultiple=7&waitQueueTimeoutMS=150&'
                + 'maxIdleTimeMS=200&maxLifeTimeMS=300&replicaSet=test&'
                + 'connectTimeoutMS=2500&socketTimeoutMS=5500&'
                + 'safe=false&w=1&wtimeout=2500&fsync=true&ssl=true&readPreference=secondary&'
                + 'sslInvalidHostNameAllowed=true&'
                + 'serverSelectionTimeoutMS=25000&'
                + 'localThresholdMS=30&'
                + 'heartbeatFrequencyMS=20000&'
                + 'appName=app1')

        when:
        def options = uri.getOptions()

        then:
        options.getWriteConcern() == new WriteConcern(1, 2500, true)
        options.getReadPreference() == ReadPreference.secondary()
        options.getConnectionsPerHost() == 10
        options.getMinConnectionsPerHost() == 5
        options.getMaxWaitTime() == 150
        options.getThreadsAllowedToBlockForConnectionMultiplier() == 7
        options.getMaxConnectionIdleTime() == 200
        options.getMaxConnectionLifeTime() == 300
        options.getSocketTimeout() == 5500
        options.getConnectTimeout() == 2500
        options.getRequiredReplicaSetName() == 'test'
        options.isSslEnabled()
        options.isSslInvalidHostNameAllowed()
        options.getServerSelectionTimeout() == 25000
        options.getLocalThreshold() == 30
        options.getHeartbeatFrequency() == 20000
        options.getApplicationName() == 'app1'
    }

    def 'should have correct defaults for options'() {
        when:
        MongoClientOptions options = new MongoClientURI('mongodb://localhost').getOptions();

        then:
        options.getConnectionsPerHost() == 100;
        options.getThreadsAllowedToBlockForConnectionMultiplier() == 5;
        options.getMaxWaitTime() == 120000;
        options.getConnectTimeout() == 10000;
        options.getSocketTimeout() == 0;
        !options.isSocketKeepAlive();
        options.getDescription() == null;
        options.getReadPreference() == ReadPreference.primary();
        options.getRequiredReplicaSetName() == null
        !options.isSslEnabled()
    }

    @Unroll
    def 'should support all credential types'() {
        expect:
        uri.credentials == credentialList

        where:
        uri                                                   | credentialList
        new MongoClientURI('mongodb://jeff:123@localhost')    | createCredential('jeff', 'admin', '123'.toCharArray())
        new MongoClientURI('mongodb://jeff:123@localhost/?' +
                           'authMechanism=MONGODB-CR')        | createMongoCRCredential('jeff', 'admin', '123'.toCharArray())
        new MongoClientURI('mongodb://jeff:123@localhost/?' +
                           'authMechanism=MONGODB-CR' +
                           '&authSource=test')                | createMongoCRCredential('jeff', 'test', '123'.toCharArray())
        new MongoClientURI('mongodb://jeff:123@localhost/?' +
                           'authMechanism=SCRAM-SHA-1')       | createScramSha1Credential('jeff', 'admin', '123'.toCharArray())
        new MongoClientURI('mongodb://jeff:123@localhost/?' +
                           'authMechanism=SCRAM-SHA-1' +
                           '&authSource=test')                | createScramSha1Credential('jeff', 'test', '123'.toCharArray())
        new MongoClientURI('mongodb://jeff@localhost/?' +
                           'authMechanism=GSSAPI')            | createGSSAPICredential('jeff')
        new MongoClientURI('mongodb://jeff:123@localhost/?' +
                           'authMechanism=PLAIN')             | createPlainCredential('jeff', 'admin', '123'.toCharArray())
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
        uri.getOptions().getReadPreference() == readPreference;

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

    @IgnoreIf({ isNotAtLeastJava7() })
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
                         + 'maxPoolSize=10;waitQueueMultiple=7;'
                         + 'waitQueueTimeoutMS=150;'
                         + 'maxIdleTimeMS=200;'
                         + 'maxLifeTimeMS=300;replicaSet=test;'
                         + 'connectTimeoutMS=2500;'
                         + 'socketTimeoutMS=5500;'
                         + 'safe=false;w=1;wtimeout=2500;'
                         + 'fsync=true;readPreference=primary;'
                         + 'ssl=true')                               |  new MongoClientURI('mongodb://localhost/db.coll?minPoolSize=5;'
                                                                                         + 'maxPoolSize=10&waitQueueMultiple=7;'
                                                                                         + 'waitQueueTimeoutMS=150;'
                                                                                         + 'maxIdleTimeMS=200&maxLifeTimeMS=300'
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
                         + '&readPreferenceTags=')                  | new MongoClientURI('mongodb://localhost/'
                                                                                       + '?readPreference=secondaryPreferred'
                                                                                       + '&readPreferenceTags=dc:ny'
                                                                                       + '&readPreferenceTags=dc:ny, rack:1'
                                                                                       + '&readPreferenceTags=')
        new MongoClientURI('mongodb://ross:123@localhost/?'
                         + 'authMechanism=SCRAM-SHA-1')            | new MongoClientURI('mongodb://ross:123@localhost/?'
                                                                                       + 'authMechanism=GSSAPI')
    }

    def 'should be equal to another MongoClientURI with options'() {
        when:
        MongoClientURI uri1 = new MongoClientURI('mongodb://user:pass@host1:1,host2:2,host3:3/bar?'
                                                        + 'maxPoolSize=10;waitQueueMultiple=5;waitQueueTimeoutMS=150;'
                                                        + 'minPoolSize=7;maxIdleTimeMS=1000;maxLifeTimeMS=2000;replicaSet=test;'
                                                        + 'connectTimeoutMS=2500;socketTimeoutMS=5500;autoConnectRetry=true;'
                                                        + 'slaveOk=true;safe=false;w=1;wtimeout=2600;fsync=true')

        MongoClientOptions.Builder builder = MongoClientOptions.builder()
                                                               .connectionsPerHost(10)
                                                               .threadsAllowedToBlockForConnectionMultiplier(5)
                                                               .maxWaitTime(150)
                                                               .minConnectionsPerHost(7)
                                                               .maxConnectionIdleTime(1000)
                                                               .maxConnectionLifeTime(2000)
                                                               .requiredReplicaSetName('test')
                                                               .connectTimeout(2500)
                                                               .socketTimeout(5500)
                                                               .readPreference(secondaryPreferred())
                                                               .writeConcern(new WriteConcern(1, 2600, true))

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
