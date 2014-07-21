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

package org.mongodb

import com.mongodb.ReadPreference
import com.mongodb.Tags
import com.mongodb.WriteConcern
import com.mongodb.client.MongoClientOptions
import com.mongodb.client.MongoClientURI
import spock.lang.Specification
import spock.lang.Unroll

import static com.mongodb.MongoCredential.createGSSAPICredential
import static com.mongodb.MongoCredential.createMongoCRCredential
import static com.mongodb.MongoCredential.createMongoX509Credential
import static com.mongodb.MongoCredential.createPlainCredential
import static com.mongodb.ReadPreference.secondaryPreferred
import static java.util.Arrays.asList

class MongoClientURISpecification extends Specification {
    def 'should throw Exception if URI does not have a trailing slash'() {
        when:
        new MongoClientURI('mongodb://localhost?wTimeout=5');

        then:
        thrown(IllegalArgumentException)
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
                                   'host3:9/bar')      | 3   | ['host:7',
                                                                'host2:8',
                                                                'host3:9']        | 'bar'    | null       | 'user'   | 'pass' as char[]
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
        uri                                                                                | writeConcern
        new MongoClientURI('mongodb://localhost')                                          | WriteConcern.ACKNOWLEDGED
        new MongoClientURI('mongodb://localhost/?safe=true')                               | WriteConcern.ACKNOWLEDGED
        new MongoClientURI('mongodb://localhost/?safe=false')                              | WriteConcern.UNACKNOWLEDGED
        new MongoClientURI('mongodb://localhost/?wTimeout=5')                              | new WriteConcern(1, 5, false, false)
        new MongoClientURI('mongodb://localhost/?fsync=true')                              | new WriteConcern(1, 0, true, false)
        new MongoClientURI('mongodb://localhost/?j=true')                                  | new WriteConcern(1, 0, false, true)
        new MongoClientURI('mongodb://localhost/?w=2&wtimeout=5&fsync=true&j=true')        | new WriteConcern(2, 5, true, true)
        new MongoClientURI('mongodb://localhost/?w=majority&wtimeout=5&fsync=true&j=true') | new WriteConcern('majority', 5, true, true)
    }

    @Unroll
    def 'should correctly parse URI options for #type'() {
        expect:
        options.getMinConnectionPoolSize() == 5
        options.getMaxConnectionPoolSize() == 10;
        options.getThreadsAllowedToBlockForConnectionMultiplier() == 5;
        options.getMaxWaitTime() == 150;
        options.getMaxConnectionIdleTime() == 200
        options.getMaxConnectionLifeTime() == 300
        options.getSocketTimeout() == 5500;
        options.getWriteConcern() == new WriteConcern(1, 2500, true);
        options.getReadPreference() == ReadPreference.primary() ;
        options.getRequiredReplicaSetName() == 'test'

        where:
        options <<
                [new MongoClientURI('mongodb://localhost/?minPoolSize=5&maxPoolSize=10&waitQueueMultiple=5&waitQueueTimeoutMS=150&'
                                            + 'maxIdleTimeMS=200&maxLifeTimeMS=300&replicaSet=test&'
                                            + 'connectTimeoutMS=2500&socketTimeoutMS=5500&'
                                            + 'safe=false&w=1&wtimeout=2500&fsync=true').getOptions(),
                 new MongoClientURI('mongodb://localhost/?minPoolSize=5;maxPoolSize=10;waitQueueMultiple=5;waitQueueTimeoutMS=150;'
                                            + 'maxIdleTimeMS=200;maxLifeTimeMS=300;replicaSet=test;'
                                            + 'connectTimeoutMS=2500;socketTimeoutMS=5500;'
                                            + 'safe=false;w=1;wtimeout=2500;fsync=true').getOptions(),
                 new MongoClientURI('mongodb://localhost/test?minPoolSize=5;maxPoolSize=10&waitQueueMultiple=5;waitQueueTimeoutMS=150;'
                                            + 'maxIdleTimeMS=200&maxLifeTimeMS=300&replicaSet=test;'
                                            + 'connectTimeoutMS=2500;'
                                            + 'socketTimeoutMS=5500&'
                                            + 'safe=false&w=1;wtimeout=2500;fsync=true').getOptions()]
        //for documentation, i.e. the Unroll description for each type
        type << ['amp', 'semi', 'mixed']
    }

    def 'should have correct defaults for options'() {
        when:
        MongoClientOptions options = new MongoClientURI('mongodb://localhost').getOptions();

        then:
        options.getMaxConnectionPoolSize() == 100;
        options.getThreadsAllowedToBlockForConnectionMultiplier() == 5;
        options.getMaxWaitTime() == 120000;
        options.getConnectTimeout() == 10000;
        options.getSocketTimeout() == 0;
        !options.isSocketKeepAlive();
        options.getDescription() == null;
        options.getReadPreference() == ReadPreference.primary();
        options.getRequiredReplicaSetName() == null
    }

    @Unroll
    def 'should support all credential types'() {
        expect:
        uri.credentialList == credentialList

        where:
        uri                                                   | credentialList
        new MongoClientURI('mongodb://jeff:123@localhost')    | asList(createMongoCRCredential('jeff', 'admin', '123'.toCharArray()))
        new MongoClientURI('mongodb://jeff:123@localhost/?' +
                           'authMechanism=MONGODB-CR')        | asList(createMongoCRCredential('jeff', 'admin', '123'.toCharArray()))
        new MongoClientURI('mongodb://jeff:123@localhost/?' +
                           'authMechanism=MONGODB-CR' +
                           '&authSource=test')                | asList(createMongoCRCredential('jeff', 'test', '123'.toCharArray()))
        new MongoClientURI('mongodb://jeff@localhost/?' +
                           'authMechanism=GSSAPI')            | asList(createGSSAPICredential('jeff'))
        new MongoClientURI('mongodb://jeff:123@localhost/?' +
                           'authMechanism=PLAIN')             | asList(createPlainCredential('jeff', 'admin', '123'.toCharArray()))
        new MongoClientURI('mongodb://jeff@localhost/?' +
                           'authMechanism=MONGODB-X509')      | asList(createMongoX509Credential('jeff'))
        new MongoClientURI('mongodb://jeff@localhost/?' +
                           'authMechanism=GSSAPI' +
                           '&gssapiServiceName=foo')          | asList(createGSSAPICredential('jeff')
                                                                                       .withMechanismProperty('SERVICE_NAME', 'foo'))
    }

    @Unroll
    def 'should correct parse read preference for #readPreference'() {
        expect:
        uri.getOptions().getReadPreference() == readPreference;

        where:
        uri                                                              | readPreference
        new MongoClientURI('mongodb://localhost/' +
                                   '?readPreference=secondaryPreferred') | ReadPreference.secondaryPreferred()
        new MongoClientURI('mongodb://localhost/' +
                                   '?readPreference=secondaryPreferred' +
                                   '&readPreferenceTags=dc:ny,rack:1' +
                                   '&readPreferenceTags=dc:ny' +
                                   '&readPreferenceTags=')               | secondaryPreferred([new Tags('dc', 'ny').append('rack', '1'),
                                                                                               new Tags('dc', 'ny'),
                                                                                               new Tags()])
    }
}
