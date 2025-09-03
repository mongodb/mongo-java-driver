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

import org.bson.UuidRepresentation
import spock.lang.Specification
import spock.lang.Unroll

import static com.mongodb.MongoCompressor.LEVEL
import static com.mongodb.MongoCompressor.createZlibCompressor
import static com.mongodb.MongoCompressor.createZstdCompressor
import static com.mongodb.MongoCredential.createCredential
import static com.mongodb.MongoCredential.createGSSAPICredential
import static com.mongodb.MongoCredential.createMongoX509Credential
import static com.mongodb.MongoCredential.createPlainCredential
import static com.mongodb.MongoCredential.createScramSha1Credential
import static com.mongodb.MongoCredential.createScramSha256Credential
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.ReadPreference.secondaryPreferred
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS

/**
 * Update {@link ConnectionStringUnitTest} instead.
 */
class ConnectionStringSpecification extends Specification {
    static final LONG_STRING = new String((1..256).collect { (byte) 1 } as byte[])

    @Unroll
    def 'should parse #connectionString into correct components'() {
        expect:
        connectionString.getHosts().size() == num
        connectionString.getHosts() == hosts
        connectionString.getDatabase() == database
        connectionString.getCollection() == collection
        connectionString.getUsername() == username
        connectionString.getPassword() == password

        where:
        connectionString                                 | num | hosts              | database | collection | username | password
        new ConnectionString('mongodb://db.example.com') | 1   | ['db.example.com'] | null     | null       | null     | null
        new ConnectionString('mongodb://10.0.0.1')       | 1   | ['10.0.0.1']       | null     | null       | null     | null
        new ConnectionString('mongodb://[::1]')          | 1   | ['[::1]']          | null     | null       | null     | null
        new ConnectionString('mongodb://%2Ftmp%2Fmongo'
                + 'db-27017.sock')                       | 1   | ['/tmp/mongodb' +
                                                                  '-27017.sock']    | null     | null       | null     | null
        new ConnectionString('mongodb://foo/bar')        | 1   | ['foo']            | 'bar'    | null       | null     | null
        new ConnectionString('mongodb://10.0.0.1/bar')   | 1   | ['10.0.0.1']       | 'bar'    | null       | null     | null
        new ConnectionString('mongodb://[::1]/bar')      | 1   | ['[::1]']          | 'bar'    | null       | null     | null
        new ConnectionString('mongodb://%2Ftmp%2Fmongo'
                + 'db-27017.sock/bar')                   | 1   | ['/tmp/mongodb' +
                                                                  '-27017.sock']    | 'bar'    | null       | null     | null
        new ConnectionString('mongodb://localhost/' +
                                   'test.my.coll')       | 1   | ['localhost']      | 'test'   | 'my.coll'  | null     | null
        new ConnectionString('mongodb://foo/bar.goo')    | 1   | ['foo']            | 'bar'    | 'goo'      | null     | null
        new ConnectionString('mongodb://foo/s,db')       | 1   | ['foo']            | 's,db'| null      | null     | null
        new ConnectionString('mongodb://foo/s%2Cdb')     | 1   | ['foo']            | 's,db'| null      | null     | null
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
                'host3:9/bar')                          | 3   | ['10.0.0.1:7',
                                                                '[::1]:8',
                                                                'host3:9']          | 'bar'    | null       | 'user'   | 'pass' as char[]
        new ConnectionString('mongodb://user:pass@'
                + '%2Ftmp%2Fmongodb-27017.sock,'
                + '%2Ftmp%2Fmongodb-27018.sock,'
                + '%2Ftmp%2Fmongodb-27019.sock/bar')    | 3   | ['/tmp/mongodb-27017.sock',
                                                                 '/tmp/mongodb-27018.sock',
                                                                 '/tmp/mongodb-27019.sock'
                                                                ]                   | 'bar'    | null       | 'user'   | 'pass' as char[]
    }

    def 'should throw exception if mongod+srv host contains a port'() {
        when:
        new ConnectionString('mongodb+srv://host1:27017')

        then:
        thrown(IllegalArgumentException)
    }

    def 'should throw exception if mongod+srv contains multiple hosts'() {
        when:
        new ConnectionString('mongodb+srv://host1,host2')

        then:
        thrown(IllegalArgumentException)
    }

    def 'should throw exception if direct connection when using mongodb+srv'() {
        when:
        new ConnectionString('mongodb+srv://test5.test.build.10gen.cc/?directConnection=true')

        then:
        thrown(IllegalArgumentException)
    }


    def 'should correctly parse different write concerns'() {
        expect:
        uri.getWriteConcern() == writeConcern

        where:
        uri                                                                                  | writeConcern
        new ConnectionString('mongodb://localhost')                                          | null
        new ConnectionString('mongodb://localhost/?safe=true')                               | WriteConcern.ACKNOWLEDGED
        new ConnectionString('mongodb://localhost/?safe=false')                              | WriteConcern.UNACKNOWLEDGED
        new ConnectionString('mongodb://localhost/?wTimeout=5')                              | WriteConcern.ACKNOWLEDGED
                                                                                                           .withWTimeout(5, MILLISECONDS)
        new ConnectionString('mongodb://localhost/?journal=true')                            | WriteConcern.ACKNOWLEDGED.withJournal(true)
        new ConnectionString('mongodb://localhost/?w=2&wtimeout=5&journal=true')  | new WriteConcern(2, 5).withJournal(true)
        new ConnectionString('mongodb://localhost/?w=majority&wtimeout=5&j=true') | new WriteConcern('majority')
                .withWTimeout(5, MILLISECONDS).withJournal(true)
    }

    @Unroll
    def 'should treat trailing slash before query parameters as optional'() {
        expect:
        uri.getApplicationName() == appName
        uri.getDatabase() == db

        where:
        uri                                                              | appName | db
        new ConnectionString('mongodb://mongodb.com')                    | null    | null
        new ConnectionString('mongodb://mongodb.com?')                   | null    | null
        new ConnectionString('mongodb://mongodb.com/')                   | null    | null
        new ConnectionString('mongodb://mongodb.com/?')                  | null    | null
        new ConnectionString('mongodb://mongodb.com/test')               | null    | "test"
        new ConnectionString('mongodb://mongodb.com/test?')              | null    | "test"
        new ConnectionString('mongodb://mongodb.com/?appName=a1')        | "a1"    | null
        new ConnectionString('mongodb://mongodb.com?appName=a1')         | "a1"    | null
        new ConnectionString('mongodb://mongodb.com/?appName=a1/a2')     | "a1/a2" | null
        new ConnectionString('mongodb://mongodb.com?appName=a1/a2')      | "a1/a2" | null
        new ConnectionString('mongodb://mongodb.com/test?appName=a1')    | "a1"    | "test"
        new ConnectionString('mongodb://mongodb.com/test?appName=a1/a2') | "a1/a2" | "test"
    }

    def 'should correctly parse different UUID representations'() {
        expect:
        uri.getUuidRepresentation() == uuidRepresentation

        where:
        uri                                                                           | uuidRepresentation
        new ConnectionString('mongodb://localhost')                                   | null
        new ConnectionString('mongodb://localhost/?uuidRepresentation=unspecified')   | UuidRepresentation.UNSPECIFIED
        new ConnectionString('mongodb://localhost/?uuidRepresentation=javaLegacy')    | UuidRepresentation.JAVA_LEGACY
        new ConnectionString('mongodb://localhost/?uuidRepresentation=csharpLegacy')  | UuidRepresentation.C_SHARP_LEGACY
        new ConnectionString('mongodb://localhost/?uuidRepresentation=pythonLegacy')  | UuidRepresentation.PYTHON_LEGACY
        new ConnectionString('mongodb://localhost/?uuidRepresentation=standard')      | UuidRepresentation.STANDARD
    }

    @Unroll
    def 'should correctly parse retryWrites'() {
        expect:
        uri.getRetryWritesValue() == retryWritesValue

        where:
        uri                                                                           | retryWritesValue
        new ConnectionString('mongodb://localhost/')                   | null
        new ConnectionString('mongodb://localhost/?retryWrites=false') | false
        new ConnectionString('mongodb://localhost/?retryWrites=true')  | true
        new ConnectionString('mongodb://localhost/?retryWrites=foos')  | null
    }

    @Unroll
    def 'should parse range of boolean values'() {
        expect:
        uri.getSslEnabled() == value

        where:
        uri                                                                   | value
        new ConnectionString('mongodb://localhost/?tls=true')  | true
        new ConnectionString('mongodb://localhost/?tls=yes')   | true
        new ConnectionString('mongodb://localhost/?tls=1')     | true
        new ConnectionString('mongodb://localhost/?tls=false') | false
        new ConnectionString('mongodb://localhost/?tls=no')    | false
        new ConnectionString('mongodb://localhost/?tls=0')     | false
        new ConnectionString('mongodb://localhost/?tls=foo')   | null
        new ConnectionString('mongodb://localhost')            | null
    }

    @Unroll
    def 'should correct parse retryReads'() {
        expect:
        uri.getRetryReads() == retryReads

        where:
        uri                                                                           | retryReads
        new ConnectionString('mongodb://localhost/')                    | null
        new ConnectionString('mongodb://localhost/?retryReads=false')   | false
        new ConnectionString('mongodb://localhost/?retryReads=true')    | true
        new ConnectionString('mongodb://localhost/?retryReads=foos')    | null
    }

    @Unroll
    def 'should correctly parse URI options for #type'() {
        expect:
        connectionString.getMinConnectionPoolSize() == 5
        connectionString.getMaxConnectionPoolSize() == 10
        connectionString.getMaxWaitTime() == 150
        connectionString.getMaxConnectionIdleTime() == 200
        connectionString.getMaxConnectionLifeTime() == 300
        connectionString.getMaxConnecting() == 1
        connectionString.getConnectTimeout() == 2500
        connectionString.getSocketTimeout() == 5500
        connectionString.getWriteConcern() == new WriteConcern(1, 2500)
        connectionString.getReadPreference() == primary()
        connectionString.getRequiredReplicaSetName() == 'test'
        connectionString.getSslEnabled()
        connectionString.getSslInvalidHostnameAllowed()
        connectionString.getServerSelectionTimeout() == 25000
        connectionString.getLocalThreshold() == 30
        connectionString.getHeartbeatFrequency() == 20000
        connectionString.getApplicationName() == 'app1'

        where:
        connectionString <<
                [new ConnectionString('mongodb://localhost/?minPoolSize=5&maxPoolSize=10&waitQueueTimeoutMS=150&'
                                            + 'maxIdleTimeMS=200&maxLifeTimeMS=300&maxConnecting=1&replicaSet=test&'
                                            + 'connectTimeoutMS=2500&socketTimeoutMS=5500&'
                                            + 'safe=false&w=1&wtimeout=2500&readPreference=primary&ssl=true&'
                                            + 'sslInvalidHostNameAllowed=true&'
                                            + 'serverSelectionTimeoutMS=25000&'
                                            + 'localThresholdMS=30&'
                                            + 'heartbeatFrequencyMS=20000&'
                                            + 'appName=app1'),
                 new ConnectionString('mongodb://localhost/?minPoolSize=5;maxPoolSize=10;waitQueueTimeoutMS=150;'
                                            + 'maxIdleTimeMS=200;maxLifeTimeMS=300;maxConnecting=1;replicaSet=test;'
                                            + 'connectTimeoutMS=2500;socketTimeoutMS=5500;'
                                            + 'safe=false;w=1;wtimeout=2500;readPreference=primary;ssl=true;'
                                            + 'sslInvalidHostNameAllowed=true;'
                                            + 'serverSelectionTimeoutMS=25000;'
                                            + 'localThresholdMS=30;'
                                            + 'heartbeatFrequencyMS=20000;'
                                            + 'appName=app1'),
                 new ConnectionString('mongodb://localhost/test?minPoolSize=5;maxPoolSize=10;waitQueueTimeoutMS=150;'
                                            + 'maxIdleTimeMS=200&maxLifeTimeMS=300&maxConnecting=1&replicaSet=test;'
                                            + 'connectTimeoutMS=2500;'
                                            + 'socketTimeoutMS=5500&'
                                            + 'safe=false&w=1;wtimeout=2500;readPreference=primary;ssl=true;'
                                            + 'sslInvalidHostNameAllowed=true;'
                                            + 'serverSelectionTimeoutMS=25000&'
                                            + 'localThresholdMS=30;'
                                            + 'heartbeatFrequencyMS=20000&'
                                            + 'appName=app1')]
        //for documentation, i.e. the Unroll description for each type
        type << ['amp', 'semi', 'mixed']
    }

    def 'should parse options to enable TLS'() {
        when:
        def connectionString = new ConnectionString('mongodb://localhost/?ssl=false')

        then:
        connectionString.getSslEnabled() == false

        when:
        connectionString = new ConnectionString('mongodb://localhost/?ssl=true')

        then:
        connectionString.getSslEnabled()

        when:
        connectionString = new ConnectionString('mongodb://localhost/?ssl=foo')

        then:
        connectionString.getSslEnabled() == null

        when:
        connectionString = new ConnectionString('mongodb://localhost/?tls=false')

        then:
        connectionString.getSslEnabled() == false

        when:
        connectionString = new ConnectionString('mongodb://localhost/?tls=true')

        then:
        connectionString.getSslEnabled()

        when:
        connectionString = new ConnectionString('mongodb://localhost/?tls=foo')

        then:
        connectionString.getSslEnabled() == null

        when:
        connectionString = new ConnectionString('mongodb://localhost/?tls=true&ssl=false')

        then:
        thrown(IllegalArgumentException)

        when:
        connectionString = new ConnectionString('mongodb://localhost/?tls=false&ssl=true')

        then:
        thrown(IllegalArgumentException)
    }

    def 'should parse options to enable TLS invalid host names'() {
        when:
        def connectionString = new ConnectionString('mongodb://localhost/?ssl=true&sslInvalidHostNameAllowed=false')

        then:
        connectionString.getSslInvalidHostnameAllowed() == false

        when:
        connectionString = new ConnectionString('mongodb://localhost/?ssl=true&sslInvalidHostNameAllowed=true')

        then:
        connectionString.getSslInvalidHostnameAllowed()

        when:
        connectionString = new ConnectionString('mongodb://localhost/?ssl=true&sslInvalidHostNameAllowed=foo')

        then:
        connectionString.getSslInvalidHostnameAllowed() == null

        when:
        connectionString = new ConnectionString('mongodb://localhost/?tls=true&tlsAllowInvalidHostnames=false')

        then:
        connectionString.getSslInvalidHostnameAllowed() == false

        when:
        connectionString = new ConnectionString('mongodb://localhost/?tls=true&tlsAllowInvalidHostnames=true')

        then:
        connectionString.getSslInvalidHostnameAllowed()

        when:
        connectionString = new ConnectionString('mongodb://localhost/?tls=true&tlsAllowInvalidHostnames=foo')

        then:
        connectionString.getSslInvalidHostnameAllowed() == null

        when:
        connectionString = new ConnectionString(
                'mongodb://localhost/?tls=true&tlsAllowInvalidHostnames=false&sslInvalidHostNameAllowed=true')

        then:
        connectionString.getSslInvalidHostnameAllowed() == false

        when:
        connectionString = new ConnectionString(
                'mongodb://localhost/?tls=true&tlsAllowInvalidHostnames=true&sslInvalidHostNameAllowed=false')

        then:
        connectionString.getSslInvalidHostnameAllowed()
    }

    def 'should parse options to enable unsecured TLS'() {
        when:
        def connectionString = new ConnectionString('mongodb://localhost/?tls=true&tlsInsecure=true')

        then:
        connectionString.getSslInvalidHostnameAllowed()

        when:
        connectionString = new ConnectionString('mongodb://localhost/?tls=true&tlsInsecure=false')

        then:
        connectionString.getSslInvalidHostnameAllowed() == false

        when:
        connectionString = new ConnectionString('mongodb://localhost/?tls=true&tlsAllowInvalidHostnames=false')

        then:
        connectionString.getSslInvalidHostnameAllowed() == false

        when:
        connectionString = new ConnectionString('mongodb://localhost/?tls=true&tlsAllowInvalidHostnames=true')

        then:
        connectionString.getSslInvalidHostnameAllowed()
    }

    @Unroll
    def 'should throw IllegalArgumentException when the proxy settings are invalid'() {
        when:
        new ConnectionString(connectionString)

        then:
        IllegalArgumentException exception = thrown(IllegalArgumentException)
        assert exception.message == cause

        where:
        cause                                                    | connectionString
        'proxyPort can only be specified with proxyHost'         | 'mongodb://localhost:27017/?proxyPort=1'
        'proxyPort should be within the valid range (0 to 65535)'| 'mongodb://localhost:27017/?proxyHost=a&proxyPort=-1'
        'proxyPort should be within the valid range (0 to 65535)'| 'mongodb://localhost:27017/?proxyHost=a&proxyPort=65536'
        'proxyUsername can only be specified with proxyHost'     | 'mongodb://localhost:27017/?proxyUsername=1'
        'proxyUsername cannot be empty'                          | 'mongodb://localhost:27017/?proxyHost=a&proxyUsername='
        'proxyPassword can only be specified with proxyHost'     | 'mongodb://localhost:27017/?proxyPassword=1'
        'proxyPassword cannot be empty'                          | 'mongodb://localhost:27017/?proxyHost=a&proxyPassword='
        'username\'s length in bytes cannot be greater than 255' | 'mongodb://localhost:27017/?proxyHost=a&proxyUsername=' + LONG_STRING
        'password\'s length in bytes cannot be greater than 255' | 'mongodb://localhost:27017/?proxyHost=a&proxyPassword=' + LONG_STRING
        'Both proxyUsername' +
                ' and proxyPassword must be set together.' +
                ' They cannot be set individually'               | 'mongodb://localhost:27017/?proxyHost=a&proxyPassword=1'
    }

    @Unroll
    def 'should create connection string with valid proxy socket settings'() {
        when:
        def connectionString = new ConnectionString(uri)

        then:
        assert connectionString.getProxyHost() == proxyHost
        assert connectionString.getProxyPort() == 1081

        where:
        uri                                                                               |  proxyHost
        'mongodb://localhost:27017/?proxyHost=2001:db8:85a3::8a2e:370:7334&proxyPort=1081'| '2001:db8:85a3::8a2e:370:7334'
        'mongodb://localhost:27017/?proxyHost=::5000&proxyPort=1081'                      | '::5000'
        'mongodb://localhost:27017/?proxyHost=%3A%3A5000&proxyPort=1081'                  | '::5000'
        'mongodb://localhost:27017/?proxyHost=0::1&proxyPort=1081'                        | '0::1'
        'mongodb://localhost:27017/?proxyHost=hyphen-domain.com&proxyPort=1081'           | 'hyphen-domain.com'
        'mongodb://localhost:27017/?proxyHost=sub.domain.c.com.com&proxyPort=1081'        | 'sub.domain.c.com.com'
        'mongodb://localhost:27017/?proxyHost=192.168.0.1&proxyPort=1081'                 | '192.168.0.1'
    }

    @Unroll
    def 'should create connection string with valid proxy credentials settings'() {
        when:
        def connectionString = new ConnectionString(uri)

        then:
        assert connectionString.getProxyPassword() == proxyPassword
        assert connectionString.getProxyUsername() == proxyUsername

        where:
        uri                                                                                               |  proxyPassword | proxyUsername
        'mongodb://localhost:27017/?proxyHost=test4&proxyPassword=pass%21wor%24&proxyUsername=user%21name'| 'pass!wor$'    | 'user!name'
        'mongodb://localhost:27017/?proxyHost=::5000&proxyPassword=pass!wor$&proxyUsername=user!name'     | 'pass!wor$'    | 'user!name'
    }

    def 'should set proxy settings properties'() {
        when:
        def connectionString =  new ConnectionString('mongodb+srv://test5.cc/?'
                + 'proxyPort=1080'
                + '&proxyHost=proxy.com'
                + '&proxyUsername=username'
                + '&proxyPassword=password')

        then:
        connectionString.getProxyHost() == 'proxy.com'
        connectionString.getProxyPort() == 1080
        connectionString.getProxyUsername() == 'username'
        connectionString.getProxyPassword() == 'password'
    }


    @Unroll
    def 'should throw IllegalArgumentException when the string #cause'() {
        when:
        new ConnectionString(connectionString)

        then:
        thrown(IllegalArgumentException)

        where:

        cause                                           | connectionString
        'is not a connection string'                    | 'hello world'
        'is missing a host'                             | 'mongodb://'
        'has an empty host'                             | 'mongodb://localhost:27017,,localhost:27019'
        'has an malformed IPv6 host'                    | 'mongodb://[::1'
        'has unescaped colons'                          | 'mongodb://locahost::1'
        'contains an invalid port string'               | 'mongodb://localhost:twenty'
        'contains an invalid port negative'             | 'mongodb://localhost:-1'
        'contains an invalid port out of range'         | 'mongodb://localhost:1000000'
        'contains multiple at-signs'                    | 'mongodb://user@123:password@localhost'
        'contains multiple colons'                      | 'mongodb://user:123:password@localhost'
        'invalid integer in options'                    | 'mongodb://localhost/?wTimeout=five'
        'has incomplete options'                        | 'mongodb://localhost/?wTimeout'
        'has an unknown auth mechanism'                 | 'mongodb://user:password@localhost/?authMechanism=postItNote'
        'invalid readConcern'                           | 'mongodb://localhost:27017/?readConcernLevel=pickThree'
        'contains tags but no mode'                     | 'mongodb://localhost:27017/?readPreferenceTags=dc:ny'
        'contains max staleness but no mode'            | 'mongodb://localhost:27017/?maxStalenessSeconds=100.5'
        'contains tags and primary mode'                | 'mongodb://localhost:27017/?readPreference=primary&readPreferenceTags=dc:ny'
        'contains max staleness and primary mode'       | 'mongodb://localhost:27017/?readPreference=primary&maxStalenessSeconds=100'
        'contains non-integral max staleness'           | 'mongodb://localhost:27017/?readPreference=secondary&maxStalenessSeconds=100.0'
        'contains GSSAPI mechanism with no user'        | 'mongodb://localhost:27017/?authMechanism=GSSAPI'
        'contains SCRAM mechanism with no user'         | 'mongodb://localhost:27017/?authMechanism=SCRAM-SHA-1'
        'contains MONGODB mechanism with no user'       | 'mongodb://localhost:27017/?authMechanism=MONGODB-CR'
        'contains PLAIN mechanism with no user'         | 'mongodb://localhost:27017/?authMechanism=PLAIN'
        'contains multiple hosts and directConnection'  | 'mongodb://localhost:27017,localhost:27018/?directConnection=true'
    }

    def 'should have correct defaults for options'() {
        when:
        def connectionString = new ConnectionString('mongodb://localhost')

        then:
        connectionString.getMaxConnectionPoolSize() == null
        connectionString.getMaxWaitTime() == null
        connectionString.getMaxConnecting() == null
        connectionString.getConnectTimeout() == null
        connectionString.getSocketTimeout() == null
        connectionString.getWriteConcern() == null
        connectionString.getReadConcern() == null
        connectionString.getReadPreference() == null
        connectionString.getRequiredReplicaSetName() == null
        connectionString.getSslEnabled() == null
        connectionString.getSslInvalidHostnameAllowed() == null
        connectionString.getApplicationName() == null
        connectionString.getCompressorList() == []
        connectionString.getRetryWritesValue() == null
        connectionString.getRetryReads() == null
    }

    @Unroll
    def 'should support all credential types'() {
        expect:
        uri.credential == credential

        where:
        uri                                                   | credential
        new ConnectionString('mongodb://jeff:123@localhost')  | createCredential('jeff', 'admin', '123'.toCharArray())
        new ConnectionString('mongodb://jeff:123@localhost/?' +
                           '&authSource=test')                | createCredential('jeff', 'test', '123'.toCharArray())
        new ConnectionString('mongodb://jeff:123@localhost/?' +
                           'authMechanism=MONGODB-CR')        | createCredential('jeff', 'admin', '123'.toCharArray())
        new ConnectionString('mongodb://jeff:123@localhost/?' +
                           'authMechanism=MONGODB-CR' +
                           '&authSource=test')                | createCredential('jeff', 'test', '123'.toCharArray())
        new ConnectionString('mongodb://jeff:123@localhost/?' +
                             'authMechanism=SCRAM-SHA-1')     | createScramSha1Credential('jeff', 'admin', '123'.toCharArray())
        new ConnectionString('mongodb://jeff:123@localhost/?' +
                             'authMechanism=SCRAM-SHA-1' +
                             '&authSource=test')              | createScramSha1Credential('jeff', 'test', '123'.toCharArray())
        new ConnectionString('mongodb://jeff:123@localhost/?' +
                'authMechanism=SCRAM-SHA-256')                | createScramSha256Credential('jeff', 'admin', '123'.toCharArray())
        new ConnectionString('mongodb://jeff:123@localhost/?' +
                'authMechanism=SCRAM-SHA-256' +
                '&authSource=test')                           | createScramSha256Credential('jeff', 'test', '123'.toCharArray())
        new ConnectionString('mongodb://jeff@localhost/?' +
                           'authMechanism=GSSAPI')            | createGSSAPICredential('jeff')
        new ConnectionString('mongodb://jeff:123@localhost/?' +
                           'authMechanism=PLAIN')             | createPlainCredential('jeff', '$external', '123'.toCharArray())
        new ConnectionString('mongodb://jeff@localhost/?' +
                           'authMechanism=MONGODB-X509')      | createMongoX509Credential('jeff')
        new ConnectionString('mongodb://localhost/?' +
                           'authMechanism=MONGODB-X509')      | createMongoX509Credential()
        new ConnectionString('mongodb://jeff@localhost/?' +
                           'authMechanism=GSSAPI' +
                           '&gssapiServiceName=foo')          | createGSSAPICredential('jeff')
                                                                            .withMechanismProperty('SERVICE_NAME', 'foo')
        new ConnectionString('mongodb://jeff@localhost/?' +
                             'authMechanism=GSSAPI' +
                             '&authMechanismProperties=' +
                             'SERVICE_NAME:foo')              | createGSSAPICredential('jeff')
                                                                            .withMechanismProperty('SERVICE_NAME', 'foo')
        new ConnectionString('mongodb://jeff@localhost/?' +
                             'authMechanism=GSSAPI' +
                             '&authMechanismProperties=' +
                             'SERVICE_NAME :foo')              | createGSSAPICredential('jeff')
                                                                            .withMechanismProperty('SERVICE_NAME', 'foo')
        new ConnectionString('mongodb://jeff@localhost/?' +
                             'authMechanism=GSSAPI' +
                             '&authMechanismProperties=' +
                             'SERVICE_NAME:foo,' +
                             'CANONICALIZE_HOST_NAME:true,' +
                             'SERVICE_REALM:AWESOME')        | createGSSAPICredential('jeff')
                                                                          .withMechanismProperty('SERVICE_NAME', 'foo')
                                                                          .withMechanismProperty('CANONICALIZE_HOST_NAME', true)
                                                                          .withMechanismProperty('SERVICE_REALM', 'AWESOME')
    }

    def 'should ignore authSource if there is no credential'() {
        expect:
        new ConnectionString('mongodb://localhost/?authSource=test').credential == null
    }

    def 'should ignore authMechanismProperties if there is no credential'() {
        expect:
        new ConnectionString('mongodb://localhost/?&authMechanismProperties=SERVICE_REALM:AWESOME').credential == null
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
                             'SERVICE_NAMEbar') // missing =

        then:
        thrown(IllegalArgumentException)
    }

    @Unroll
    def 'should correct parse read preference for #readPreference'() {
        expect:
        uri.getReadPreference() == readPreference

        where:
        uri                                                              | readPreference
        new ConnectionString('mongodb://localhost')                      | null
        new ConnectionString('mongodb://localhost/' +
                '?readPreference=primary')                               | primary()
        new ConnectionString('mongodb://localhost/' +
                '?readPreference=secondary')                             | secondary()
        new ConnectionString('mongodb://localhost/' +
                                   '?readPreference=secondaryPreferred') | secondaryPreferred()
        new ConnectionString('mongodb://localhost/' +
                                   '?readPreference=secondaryPreferred' +
                                   '&readPreferenceTags=dc:ny,rack:1' +
                                   '&readPreferenceTags=dc:ny' +
                                   '&readPreferenceTags=')               | secondaryPreferred([new TagSet(asList(new Tag('dc', 'ny'),
                                                                                                                 new Tag('rack', '1'))),
                                                                                               new TagSet(asList(new Tag('dc', 'ny'))),
                                                                                               new TagSet()])
        new ConnectionString('mongodb://localhost/' +
                '?readPreference=secondary' +
                '&maxStalenessSeconds=120')                              | secondary(120000, MILLISECONDS)
        new ConnectionString('mongodb://localhost/' +
                '?readPreference=secondary' +
                '&maxStalenessSeconds=0')                                | secondary(0, MILLISECONDS)
        new ConnectionString('mongodb://localhost/' +
                '?readPreference=secondary' +
                '&maxStalenessSeconds=-1')                               | secondary()
        new ConnectionString('mongodb://localhost/' +
                '?readPreference=primary' +
                '&maxStalenessSeconds=-1')                               | primary()
    }

    @Unroll
    def 'should correct parse read concern for #readConcern'() {
        expect:
        uri.getReadConcern() == readConcern

        where:
        uri                                                                     | readConcern
        new ConnectionString('mongodb://localhost/')                            | null
        new ConnectionString('mongodb://localhost/?readConcernLevel=local')     | ReadConcern.LOCAL
        new ConnectionString('mongodb://localhost/?readConcernLevel=majority')  | ReadConcern.MAJORITY
    }

    @Unroll
    def 'should parse compressors'() {
        expect:
        uri.getCompressorList() == [compressor]

        where:
        uri                                                                          | compressor
        new ConnectionString('mongodb://localhost/?compressors=zlib') | createZlibCompressor()
        new ConnectionString('mongodb://localhost/?compressors=zlib' +
                '&zlibCompressionLevel=5')                                           | createZlibCompressor().withProperty(LEVEL, 5)
        new ConnectionString('mongodb://localhost/?compressors=zstd') | createZstdCompressor()
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
        new ConnectionString('mongodb://ross:123@localhost/?'
                + 'proxyHost=proxy.com'
                + '&proxyPort=1080'
                + '&proxyUsername=username'
                + '&proxyPassword=password')                         |         new ConnectionString('mongodb://ross:123@localhost/?'
                                                                                            + 'proxyHost=proxy.com'
                                                                                            + '&proxyPort=1080'
                                                                                            + '&proxyUsername=username'
                                                                                            + '&proxyPassword=password')

        new ConnectionString('mongodb://localhost/db.coll'
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
                             + 'directConnection=true;'
                             + 'ssl=true')                           |  new ConnectionString('mongodb://localhost/db.coll?minPoolSize=5;'
                                                                                             + 'maxPoolSize=10;'
                                                                                             + 'waitQueueTimeoutMS=150;'
                                                                                             + 'maxIdleTimeMS=200&maxLifeTimeMS=300'
                                                                                             + '&replicaSet=test;'
                                                                                             + 'maxConnecting=1;'
                                                                                             + 'connectTimeoutMS=2500;'
                                                                                             + 'socketTimeoutMS=5500&safe=false&w=1;'
                                                                                             + 'wtimeout=2500;fsync=true'
                                                                                             + '&directConnection=true'
                                                                                             + '&readPreference=primary;ssl=true')
    }

    def 'should be not equal to another ConnectionString with the different string values'() {
        expect:
        uri1 != uri2
        uri1.hashCode() != uri2.hashCode()

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
                           + '&readPreferenceTags='
                           + '&maxConnecting=1')                | new ConnectionString('mongodb://localhost/'
                                                                                           + '?readPreference=secondaryPreferred'
                                                                                           + '&readPreferenceTags=dc:ny'
                                                                                           + '&readPreferenceTags=dc:ny, rack:1'
                                                                                           + '&readPreferenceTags='
                                                                                           + '&maxConnecting=2')
        new ConnectionString('mongodb://ross:123@localhost/?'
                           + 'authMechanism=SCRAM-SHA-1')             | new ConnectionString('mongodb://ross:123@localhost/?'
                                                                                          + 'authMechanism=GSSAPI')
        new ConnectionString('mongodb://ross:123@localhost/?'
                + 'proxyHost=proxy.com')                              |     new ConnectionString('mongodb://ross:123@localhost/?'
                                                                                        + 'proxyHost=1proxy.com')
        new ConnectionString('mongodb://ross:123@localhost/?'
                + 'proxyHost=proxy.com&proxyPort=1080')               |     new ConnectionString('mongodb://ross:123@localhost/?'
                                                                                         + 'proxyHost=proxy.com1.com&proxyPort=1081')
        new ConnectionString('mongodb://ross:123@localhost/?'
                + 'proxyHost=proxy.com&proxyPassword=password'
                + '&proxyUsername=username')                            |     new ConnectionString('mongodb://ross:123@localhost/?'
                                                                                         + 'proxyHost=proxy.com&proxyPassword=password1'
                                                                                         + '&proxyUsername=username')
    }

    def 'should recognize SRV protocol'() {
        when:
        def connectionString = new ConnectionString('mongodb+srv://test5.test.build.10gen.cc')

        then:
        connectionString.isSrvProtocol()
        connectionString.hosts == ['test5.test.build.10gen.cc']
    }

    // sslEnabled defaults to true with mongodb+srv but can be overridden via query parameter
    def 'should set sslEnabled property with SRV protocol'() {
        expect:
        connectionString.getSslEnabled() == sslEnabled

        where:
        connectionString                                                           | sslEnabled
        new ConnectionString('mongodb+srv://test5.test.build.10gen.cc')            | true
        new ConnectionString('mongodb+srv://test5.test.build.10gen.cc/?tls=true')  | true
        new ConnectionString('mongodb+srv://test5.test.build.10gen.cc/?ssl=true')  | true
        new ConnectionString('mongodb+srv://test5.test.build.10gen.cc/?tls=false') | false
        new ConnectionString('mongodb+srv://test5.test.build.10gen.cc/?ssl=false') | false
    }


    // these next two tests are functionally part of the initial-dns-seedlist-discovery specification tests, but since those
    // tests require that the driver connects to an actual replica set, it isn't possible to create specification tests
    // with URIs containing user names, since connection to a replica set that doesn't have that user defined would fail.
    // So to ensure there is proper test coverage of an authSource property specified in a TXT record, adding those tests here.
    def 'should use authSource from TXT record'() {
        given:
        def uri = new ConnectionString('mongodb+srv://bob:pwd@test5.test.build.10gen.cc/')

        expect:
        uri.credential == createCredential('bob', 'thisDB', 'pwd'.toCharArray())
    }

    def 'should override authSource from TXT record with authSource from connectionString'() {
        given:
        def uri = new ConnectionString('mongodb+srv://bob:pwd@test5.test.build.10gen.cc/?authSource=otherDB')

        expect:
        uri.credential == createCredential('bob', 'otherDB', 'pwd'.toCharArray())
    }

    def 'should use DnsClient to resolve TXT record'() {
        given:
        def dnsClient = { def name, def type -> ['replicaSet=java'] }

        when:
        def connectionString = new ConnectionString('mongodb+srv://free-java.mongodb-dev.net', dnsClient);

        then:
        connectionString.getRequiredReplicaSetName() == 'java'
    }
}
