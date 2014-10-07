/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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

import spock.lang.Specification
import spock.lang.Unroll

import static com.mongodb.MongoCredential.createCredential
import static com.mongodb.MongoCredential.createGSSAPICredential
import static com.mongodb.MongoCredential.createMongoCRCredential
import static com.mongodb.MongoCredential.createMongoX509Credential
import static com.mongodb.MongoCredential.createPlainCredential
import static com.mongodb.MongoCredential.createScramSha1Credential
import static com.mongodb.ReadPreference.secondaryPreferred
import static java.util.Arrays.asList

class ConnectionStringSpecification extends Specification {
    def 'should throw Exception if URI does not have a trailing slash'() {
        when:
        new ConnectionString('mongodb://localhost?wTimeout=5');

        then:
        thrown(IllegalArgumentException)
    }

    @Unroll
    def 'should parse #connectionString into correct components'() {
        expect:
        connectionString.getHosts().size() == num;
        connectionString.getHosts() == hosts;
        connectionString.getDatabase() == database;
        connectionString.getCollection() == collection;
        connectionString.getUsername() == username;
        connectionString.getPassword() == password;

        where:
        connectionString                                 | num | hosts              | database | collection | username | password
        new ConnectionString('mongodb://db.example.com') | 1   | ['db.example.com'] | null     | null       | null     | null
        new ConnectionString('mongodb://10.0.0.1')       | 1   | ['10.0.0.1']       | null     | null       | null     | null
        new ConnectionString('mongodb://[::1]')          | 1   | ['[::1]']          | null     | null       | null     | null
        new ConnectionString('mongodb://foo/bar')        | 1   | ['foo']            | 'bar'    | null       | null     | null
        new ConnectionString('mongodb://10.0.0.1/bar')   | 1   | ['10.0.0.1']       | 'bar'    | null       | null     | null
        new ConnectionString('mongodb://[::1]/bar')      | 1   | ['[::1]']          | 'bar'    | null       | null     | null
        new ConnectionString('mongodb://localhost/' +
                                   'test.my.coll')       | 1   | ['localhost']      | 'test'   | 'my.coll'  | null     | null
        new ConnectionString('mongodb://foo/bar.goo')    | 1   | ['foo']            | 'bar'    | 'goo'      | null     | null
        new ConnectionString('mongodb://user:pass@' +
                                   'host/bar')           | 1   | ['host']           | 'bar'    | null       | 'user'   | 'pass' as char[]
        new ConnectionString('mongodb://user:pass@' +
                                   'host:27011/bar')     | 1   | ['host:27011']     | 'bar'    | null       | 'user'   | 'pass' as char[]
        new ConnectionString('mongodb://user:pass@' +
                           '10.0.0.1:27011/bar')         | 1   | ['10.0.0.1:27011'] | 'bar'    | null       | 'user'   | 'pass' as char[]
        new ConnectionString('mongodb://user:pass@' +
                           '[::1]:27011/bar')            | 1   | ['[::1]:27011']    | 'bar'    | null       | 'user'   | 'pass' as char[]
        new ConnectionString('mongodb://user:pass@' +
                                   'host:7,' +
                                   'host2:8,' +
                                   'host3:9/bar')        | 3   | ['host2:8',
                                                                'host3:9',
                                                                'host:7']          | 'bar'    | null       | 'user'   | 'pass' as char[]
        new ConnectionString('mongodb://user:pass@' +
                           '10.0.0.1:7,' +
                           '[::1]:8,' +
                           'host3:9/bar')                | 3   | ['10.0.0.1:7',
                                                                '[::1]:8',
                                                                'host3:9']          | 'bar'    | null       | 'user'   | 'pass' as char[]
    }

    def 'should correctly parse different write concerns'() {
        expect:
        uri.getWriteConcern() == writeConcern;

        where:
        uri                                                                                  | writeConcern
        new ConnectionString('mongodb://localhost')                                          | null
        new ConnectionString('mongodb://localhost/?safe=true')                               | WriteConcern.ACKNOWLEDGED
        new ConnectionString('mongodb://localhost/?safe=false')                              | WriteConcern.UNACKNOWLEDGED
        new ConnectionString('mongodb://localhost/?wTimeout=5')                              | new WriteConcern(1, 5, false, false)
        new ConnectionString('mongodb://localhost/?fsync=true')                              | new WriteConcern(1, 0, true, false)
        new ConnectionString('mongodb://localhost/?j=true')                                  | new WriteConcern(1, 0, false, true)
        new ConnectionString('mongodb://localhost/?w=2&wtimeout=5&fsync=true&j=true')        | new WriteConcern(2, 5, true, true)
        new ConnectionString('mongodb://localhost/?w=majority&wtimeout=5&fsync=true&j=true') | new WriteConcern('majority', 5, true, true)
    }

    @Unroll
    def 'should correctly parse URI options for #type'() {
        expect:
        connectionString.getMinConnectionPoolSize() == 5
        connectionString.getMaxConnectionPoolSize() == 10;
        connectionString.getThreadsAllowedToBlockForConnectionMultiplier() == 7;
        connectionString.getMaxWaitTime() == 150;
        connectionString.getMaxConnectionIdleTime() == 200
        connectionString.getMaxConnectionLifeTime() == 300
        connectionString.getConnectTimeout() == 2500;
        connectionString.getSocketTimeout() == 5500;
        connectionString.getWriteConcern() == new WriteConcern(1, 2500, true);
        connectionString.getReadPreference() == ReadPreference.primary() ;
        connectionString.getRequiredReplicaSetName() == 'test'
        connectionString.getSslEnabled()

        where:
        connectionString <<
                [new ConnectionString('mongodb://localhost/?minPoolSize=5&maxPoolSize=10&waitQueueMultiple=7&waitQueueTimeoutMS=150&'
                                            + 'maxIdleTimeMS=200&maxLifeTimeMS=300&replicaSet=test&'
                                            + 'connectTimeoutMS=2500&socketTimeoutMS=5500&'
                                            + 'safe=false&w=1&wtimeout=2500&fsync=true&readPreference=primary&ssl=true'),
                 new ConnectionString('mongodb://localhost/?minPoolSize=5;maxPoolSize=10;waitQueueMultiple=7;waitQueueTimeoutMS=150;'
                                            + 'maxIdleTimeMS=200;maxLifeTimeMS=300;replicaSet=test;'
                                            + 'connectTimeoutMS=2500;socketTimeoutMS=5500;'
                                            + 'safe=false;w=1;wtimeout=2500;fsync=true;readPreference=primary;ssl=true'),
                 new ConnectionString('mongodb://localhost/test?minPoolSize=5;maxPoolSize=10&waitQueueMultiple=7;waitQueueTimeoutMS=150;'
                                            + 'maxIdleTimeMS=200&maxLifeTimeMS=300&replicaSet=test;'
                                            + 'connectTimeoutMS=2500;'
                                            + 'socketTimeoutMS=5500&'
                                            + 'safe=false&w=1;wtimeout=2500;fsync=true&readPreference=primary;ssl=true')]
        //for documentation, i.e. the Unroll description for each type
        type << ['amp', 'semi', 'mixed']
    }

    def 'should have correct defaults for options'() {
        when:
        def connectionString = new ConnectionString('mongodb://localhost');

        then:
        connectionString.getMaxConnectionPoolSize() == null;
        connectionString.getThreadsAllowedToBlockForConnectionMultiplier() == null;
        connectionString.getMaxWaitTime() == null;
        connectionString.getConnectTimeout() == null;
        connectionString.getSocketTimeout() == null;
        connectionString.getWriteConcern() == null;
        connectionString.getReadPreference() == null;
        connectionString.getRequiredReplicaSetName() == null
        connectionString.getSslEnabled() == null
    }

    @Unroll
    def 'should support all credential types'() {
        expect:
        uri.credentialList == credentialList

        where:
        uri                                                   | credentialList
        new ConnectionString('mongodb://jeff:123@localhost')  | asList(createCredential('jeff', 'admin', '123'.toCharArray()))
        new ConnectionString('mongodb://jeff:123@localhost/?' +
                           '&authSource=test')                | asList(createCredential('jeff', 'test', '123'.toCharArray()))
        new ConnectionString('mongodb://jeff:123@localhost/?' +
                           'authMechanism=MONGODB-CR')        | asList(createMongoCRCredential('jeff', 'admin', '123'.toCharArray()))
        new ConnectionString('mongodb://jeff:123@localhost/?' +
                           'authMechanism=MONGODB-CR' +
                           '&authSource=test')                | asList(createMongoCRCredential('jeff', 'test', '123'.toCharArray()))
        new ConnectionString('mongodb://jeff:123@localhost/?' +
                             'authMechanism=SCRAM-SHA-1')     | asList(createScramSha1Credential('jeff', 'admin', '123'.toCharArray()))
        new ConnectionString('mongodb://jeff:123@localhost/?' +
                             'authMechanism=SCRAM-SHA-1' +
                             '&authSource=test')              | asList(createScramSha1Credential('jeff', 'test', '123'.toCharArray()))
        new ConnectionString('mongodb://jeff@localhost/?' +
                           'authMechanism=GSSAPI')            | asList(createGSSAPICredential('jeff'))
        new ConnectionString('mongodb://jeff:123@localhost/?' +
                           'authMechanism=PLAIN')             | asList(createPlainCredential('jeff', 'admin', '123'.toCharArray()))
        new ConnectionString('mongodb://jeff@localhost/?' +
                           'authMechanism=MONGODB-X509')      | asList(createMongoX509Credential('jeff'))
        new ConnectionString('mongodb://jeff@localhost/?' +
                           'authMechanism=GSSAPI' +
                           '&gssapiServiceName=foo')          | asList(createGSSAPICredential('jeff')
                                                                            .withMechanismProperty('SERVICE_NAME', 'foo'))
        new ConnectionString('mongodb://jeff@localhost/?' +
                             'authMechanism=GSSAPI' +
                             '&authMechanismProperties=' +
                             'SERVICE_NAME:foo')              | asList(createGSSAPICredential('jeff')
                                                                            .withMechanismProperty('SERVICE_NAME', 'foo'))
        new ConnectionString('mongodb://jeff@localhost/?' +
                             'authMechanism=GSSAPI' +
                             '&authMechanismProperties=' +
                             'SERVICE_NAME :foo')              | asList(createGSSAPICredential('jeff')
                                                                            .withMechanismProperty('SERVICE_NAME', 'foo'))
        new ConnectionString('mongodb://jeff@localhost/?' +
                             'authMechanism=GSSAPI' +
                             '&authMechanismProperties=' +
                             'SERVICE_NAME:foo,' +
                             'CANONICALIZE_HOST_NAME:true,' +
                             'SERVICE_REALM:AWESOME')        | asList(createGSSAPICredential('jeff')
                                                                          .withMechanismProperty('SERVICE_NAME', 'foo')
                                                                          .withMechanismProperty('CANONICALIZE_HOST_NAME', true)
                                                                          .withMechanismProperty('SERVICE_REALM', 'AWESOME'))
    }

    def 'should support thrown an IllegalArgumentException when given invalid authMechanismProperties'() {
        when:
        new ConnectionString('mongodb://jeff@localhost/?' +
                             'authMechanism=GSSAPI' +
                             '&authMechanismProperties=' +
                             'SERVICE_NAME=foo,' +
                             'CANONICALIZE_HOST_NAME=true,' +
                             'SERVICE_REALM=AWESOME')

        then:
        thrown(IllegalArgumentException)

        when:
        new ConnectionString('mongodb://jeff@localhost/?' +
                             'authMechanism=GSSAPI' +
                             '&authMechanismProperties=' +
                             'SERVICE_NAME:foo:bar')

        then:
        thrown(IllegalArgumentException)
    }

    @Unroll
    def 'should correct parse read preference for #readPreference'() {
        expect:
        uri.getReadPreference() == readPreference;

        where:
        uri                                                              | readPreference
        new ConnectionString('mongodb://localhost/' +
                                   '?readPreference=secondaryPreferred') | secondaryPreferred()
        new ConnectionString('mongodb://localhost/?slaveOk=true')        | secondaryPreferred()
        new ConnectionString('mongodb://localhost/' +
                                   '?readPreference=secondaryPreferred' +
                                   '&readPreferenceTags=dc:ny,rack:1' +
                                   '&readPreferenceTags=dc:ny' +
                                   '&readPreferenceTags=')               | secondaryPreferred([new TagSet(asList(new Tag('dc', 'ny'),
                                                                                                                 new Tag('rack', '1'))),
                                                                                               new TagSet(asList(new Tag('dc', 'ny'))),
                                                                                               new TagSet()])
    }

    def 'should be equal to another instance with the same string values'() {
        expect:
        uri1 == uri2
        uri1.hashCode() == uri2.hashCode()

        where:
        uri1                                                        | uri2
        new ConnectionString('mongodb://user:pass@host1:1/')        | new ConnectionString('mongodb://user:pass@host1:1/')
        new ConnectionString('mongodb://user:pass@host1:1,host2:2,'
                                     + 'host3:3/bar')               | new ConnectionString('mongodb://user:pass@host3:3,host1:1,'
                                                                                           + 'host2:2/bar')
        new ConnectionString('mongodb://localhost/'
                             + '?readPreference=secondaryPreferred'
                             + '&readPreferenceTags=dc:ny,rack:1'
                             + '&readPreferenceTags=dc:ny'
                             + '&readPreferenceTags=')              | new ConnectionString('mongodb://localhost/'
                                                                                           + '?readPreference=secondaryPreferred'
                                                                                           + '&readPreferenceTags=dc:ny, rack:1'
                                                                                           + '&readPreferenceTags=dc:ny'
                                                                                           + '&readPreferenceTags=')
        new ConnectionString('mongodb://localhost/?readPreference='
                                     + 'secondaryPreferred')        | new ConnectionString('mongodb://localhost/?readPreference='
                                                                                         + 'secondaryPreferred')
        new ConnectionString('mongodb://ross:123@localhost/?'
                             + 'authMechanism=SCRAM-SHA-1')         | new ConnectionString('mongodb://ross:123@localhost/?'
                                                                                           + 'authMechanism=SCRAM-SHA-1')
        new ConnectionString('mongodb://localhost/db/coll'
                             + '?minPoolSize=5;'
                             + 'maxPoolSize=10;waitQueueMultiple=7;'
                             + 'waitQueueTimeoutMS=150;'
                             + 'maxIdleTimeMS=200;'
                             + 'maxLifeTimeMS=300;replicaSet=test;'
                             + 'connectTimeoutMS=2500;'
                             + 'socketTimeoutMS=5500;'
                             + 'safe=false;w=1;wtimeout=2500;'
                             + 'fsync=true;readPreference=primary;'
                             + 'ssl=true')                           |  new ConnectionString('mongodb://localhost/db/coll?minPoolSize=5;'
                                                                                             + 'maxPoolSize=10&waitQueueMultiple=7;'
                                                                                             + 'waitQueueTimeoutMS=150;'
                                                                                             + 'maxIdleTimeMS=200&maxLifeTimeMS=300'
                                                                                             + '&replicaSet=test;connectTimeoutMS=2500;'
                                                                                             + 'socketTimeoutMS=5500&safe=false&w=1;'
                                                                                             + 'wtimeout=2500;fsync=true'
                                                                                             + '&readPreference=primary;ssl=true')
    }

    def 'should be not equal to another ConnectionString with the different string values'() {
        expect:
        uri1 != uri2

        where:
        uri1                                                        | uri2
        new ConnectionString('mongodb://user:pass@host1:1/')        | new ConnectionString('mongodb://user:pass@host1:2/')
        new ConnectionString('mongodb://user:pass@host1:1,host2:2,'
                           + 'host3:3/bar')                         | new ConnectionString('mongodb://user:pass@host1:1,host2:2,'
                                                                                         + 'host4:4/bar')
        new ConnectionString('mongodb://localhost/?readPreference='
                           + 'secondaryPreferred')                  | new ConnectionString('mongodb://localhost/?readPreference='
                                                                                         + 'secondary')
        new ConnectionString('mongodb://localhost/'
                           + '?readPreference=secondaryPreferred'
                           + '&readPreferenceTags=dc:ny,rack:1'
                           + '&readPreferenceTags=dc:ny'
                           + '&readPreferenceTags=')                  | new ConnectionString('mongodb://localhost/'
                                                                                           + '?readPreference=secondaryPreferred'
                                                                                           + '&readPreferenceTags=dc:ny'
                                                                                           + '&readPreferenceTags=dc:ny, rack:1'
                                                                                           + '&readPreferenceTags=')
        new ConnectionString('mongodb://ross:123@localhost/?'
                           + 'authMechanism=SCRAM-SHA-1')            | new ConnectionString('mongodb://ross:123@localhost/?'
                                                                                          + 'authMechanism=GSSAPI')
    }
}
