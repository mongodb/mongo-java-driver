/*
 * Copyright 2015-2016 MongoDB, Inc.
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

package com.mongodb.connection

import com.mongodb.ServerAddress
import com.mongodb.Tag
import com.mongodb.TagSet
import org.bson.types.ObjectId
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static com.mongodb.connection.DescriptionHelper.createConnectionDescription
import static com.mongodb.connection.DescriptionHelper.createServerDescription
import static org.bson.BsonDocument.parse

class DescriptionHelperSpecification extends Specification {
    private final ServerAddress serverAddress = new ServerAddress('localhost', 27018)
    private final ServerVersion serverVersion = new ServerVersion([3, 0, 0])
    private final int roundTripTime = 5000

    def setup() {
        Time.makeTimeConstant()
    }

    def cleanup() {
        Time.makeTimeMove()
    }

    def 'connection description should reflect ismaster result'() {
        def connectionId = new ConnectionId(new ServerId(new ClusterId(), serverAddress))
        expect:
        createConnectionDescription(connectionId,
                                    parse('''{
                                          ismaster : true,
                                          maxBsonObjectSize : 16777216,
                                          maxMessageSizeBytes : 48000000,
                                          maxWriteBatchSize : 1000,
                                          localTime : ISODate("2015-03-04T23:03:45.848Z"),
                                          maxWireVersion : 3,
                                          minWireVersion : 0,
                                          ok : 1
                                          }'''),
                                    parse('''{
                                          "version" : "2.6.1",
                                          "gitVersion" : "nogitversion",
                                          "OpenSSLVersion" : "",
                                          "loaderFlags" : "-fPIC -pthread -Wl,-bind_at_load -m64 -mmacosx-version-min=10.9",
                                          "allocator" : "tcmalloc",
                                          "versionArray" : [
                                          2,
                                          6,
                                          1,
                                          0
                                          ],
                                          "javascriptEngine" : "V8",
                                          "bits" : 64,
                                          "debug" : false,
                                          "maxBsonObjectSize" : 16777216,
                                          "ok" : 1
                                          }'''))
        new ConnectionDescription(connectionId, serverVersion, ServerType.STANDALONE, 1000, 16777216, 48000000, [])
    }

    def 'connection description should reflect ismaster result with compressors'() {
        def connectionId = new ConnectionId(new ServerId(new ClusterId(), serverAddress))
        expect:
        createConnectionDescription(connectionId,
                parse('''{
                                          ismaster : true,
                                          maxBsonObjectSize : 16777216,
                                          maxMessageSizeBytes : 48000000,
                                          maxWriteBatchSize : 1000,
                                          localTime : ISODate("2015-03-04T23:03:45.848Z"),
                                          maxWireVersion : 3,
                                          minWireVersion : 0,
                                          compressors : ["zlib", "snappy"],
                                          ok : 1
                                          }'''),
                parse('''{
                                          "version" : "2.6.1",
                                          "gitVersion" : "nogitversion",
                                          "OpenSSLVersion" : "",
                                          "loaderFlags" : "-fPIC -pthread -Wl,-bind_at_load -m64 -mmacosx-version-min=10.9",
                                          "allocator" : "tcmalloc",
                                          "versionArray" : [
                                          2,
                                          6,
                                          1,
                                          0
                                          ],
                                          "javascriptEngine" : "V8",
                                          "bits" : 64,
                                          "debug" : false,
                                          "maxBsonObjectSize" : 16777216,
                                          "ok" : 1
                                          }'''))
        new ConnectionDescription(connectionId, serverVersion, ServerType.STANDALONE, 1000, 16777216, 48000000,
                ['zlib', 'snappy'])
    }

    def 'server description should reflect not ok ismaster result'() {
        expect:
        createServerDescription(serverAddress,
                                parse('{ok : 0}'), serverVersion, roundTripTime) ==
                ServerDescription.builder()
                         .ok(false)
                         .address(serverAddress)
                         .state(ServerConnectionState.CONNECTED)
                         .version(serverVersion)
                         .type(ServerType.UNKNOWN)
                         .build()
    }

    def 'server description should reflect last update time'() {
        expect:
        createServerDescription(serverAddress,
                parse('{ ok : 1 }'), serverVersion, roundTripTime).getLastUpdateTime(TimeUnit.NANOSECONDS) == Time.CONSTANT_TIME

    }

    def 'server description should reflect roundTripNanos'() {
        expect:
        createServerDescription(serverAddress,
                                parse('''{
                                      ismaster : true,
                                      maxBsonObjectSize : 16777216,
                                      maxMessageSizeBytes : 48000000,
                                      maxWriteBatchSize : 1000,
                                      localTime : ISODate("2015-03-04T23:03:45.848Z"),
                                      maxWireVersion : 3,
                                      minWireVersion : 0,
                                      ok : 1
                                      }'''), serverVersion, roundTripTime).roundTripTimeNanos ==
        ServerDescription.builder()
                         .ok(true)
                         .address(serverAddress)
                         .state(ServerConnectionState.CONNECTED)
                         .version(serverVersion)
                         .maxWireVersion(3)
                         .maxDocumentSize(16777216)
                         .type(ServerType.STANDALONE)
                         .roundTripTime(roundTripTime, TimeUnit.NANOSECONDS)
                         .build().roundTripTimeNanos
    }

    def 'server description should reflect ismaster result from standalone'() {
        expect:
        createServerDescription(serverAddress,
                parse('''{
                        ismaster : true,
                        maxBsonObjectSize : 16777216,
                        maxMessageSizeBytes : 48000000,
                        maxWriteBatchSize : 1000,
                        localTime : ISODate("2015-03-04T23:03:45.848Z"),
                        maxWireVersion : 3,
                        minWireVersion : 0,
                        ok : 1
                        }'''), serverVersion, roundTripTime) ==
        ServerDescription.builder()
                         .ok(true)
                         .address(serverAddress)
                         .state(ServerConnectionState.CONNECTED)
                         .version(serverVersion)
                         .maxWireVersion(3)
                         .maxDocumentSize(16777216)
                         .type(ServerType.STANDALONE)
                         .build()
    }

    def 'server description should reflect ismaster result from secondary'() {
        expect:
        createServerDescription(new ServerAddress('localhost', 27018),
                parse('''{
                        "setName" : "replset",
                        "ismaster" : false,
                        "secondary" : true,
                        "hosts" : [
                        "localhost:27017",
                        "localhost:27019",
                        "localhost:27018"
                        ],
                        "arbiters" : [
                        "localhost:27020"
                        ],
                        "me" : "localhost:27017",
                        "maxBsonObjectSize" : 16777216,
                        "maxMessageSizeBytes" : 48000000,
                        "maxWriteBatchSize" : 1000,
                        "localTime" : ISODate("2015-03-04T23:14:07.338Z"),
                        "maxWireVersion" : 3,
                        "minWireVersion" : 0,
                        "ok" : 1
                        }'''), serverVersion, roundTripTime) ==
        ServerDescription.builder()
                         .ok(true)
                         .address(new ServerAddress('localhost', 27018))
                         .state(ServerConnectionState.CONNECTED)
                         .version(serverVersion)
                         .maxWireVersion(3)
                         .maxDocumentSize(16777216)
                         .type(ServerType.REPLICA_SET_SECONDARY)
                         .setName('replset')
                         .canonicalAddress('localhost:27017')
                         .hosts(['localhost:27017', 'localhost:27018', 'localhost:27019'] as Set)
                         .arbiters(['localhost:27020'] as Set)
                         .build()
    }

    def 'server description should reflect ismaster result with lastWriteDate'() {
        expect:
        createServerDescription(new ServerAddress('localhost', 27018),
                parse('''{
                        "setName" : "replset",
                        "ismaster" : false,
                        "secondary" : true,
                        "hosts" : [
                        "localhost:27017",
                        "localhost:27019",
                        "localhost:27018"
                        ],
                        "arbiters" : [
                        "localhost:27020"
                        ],
                        "me" : "localhost:27017",
                        "maxBsonObjectSize" : 16777216,
                        "maxMessageSizeBytes" : 48000000,
                        "maxWriteBatchSize" : 1000,
                        "localTime" : ISODate("2015-03-04T23:14:07.338Z"),
                        "maxWireVersion" : 5,
                        "minWireVersion" : 0,
                        "lastWrite" : { "lastWriteDate" : ISODate("2016-03-04T23:14:07.338Z") }
                        "ok" : 1
                        }'''), serverVersion, roundTripTime) ==
                ServerDescription.builder()
                        .ok(true)
                        .address(new ServerAddress('localhost', 27018))
                        .state(ServerConnectionState.CONNECTED)
                        .version(serverVersion)
                        .maxWireVersion(5)
                        .lastWriteDate(new Date(1457133247338L))
                        .maxDocumentSize(16777216)
                        .type(ServerType.REPLICA_SET_SECONDARY)
                        .setName('replset')
                        .canonicalAddress('localhost:27017')
                        .hosts(['localhost:27017', 'localhost:27018', 'localhost:27019'] as Set)
                        .arbiters(['localhost:27020'] as Set)
                        .build()
    }

    def 'server description should reflect ismaster result from primary'() {
        given:
        ObjectId electionId = new ObjectId();

        when:
        def serverDescription = createServerDescription(serverAddress,
                parse("""{
                        "setName" : "replset",
                        "setVersion" : 1,
                        "ismaster" : true,
                        "secondary" : false,
                        "hosts" : [
                        "localhost:27017",
                        "localhost:27019",
                        "localhost:27018"
                        ],
                        "arbiters" : [
                        "localhost:27020"
                        ],
                        "primary" : "localhost:27017",
                        "me" : "localhost:27017",
                        "maxBsonObjectSize" : 16777216,
                        "maxMessageSizeBytes" : 48000000,
                        "maxWriteBatchSize" : 1000,
                        "localTime" : ISODate("2015-03-04T23:24:18.452Z"),
                        "maxWireVersion" : 3,
                        "minWireVersion" : 0,
                        "electionId" : {\$oid : "${electionId.toHexString()}" },
                        "setVersion" : 2,
                        tags : { "dc" : "east", "use" : "production" }
                        "ok" : 1
                        }"""), serverVersion, roundTripTime)

        then:
        serverDescription ==
        ServerDescription.builder()
                         .ok(true)
                         .address(serverAddress)
                         .state(ServerConnectionState.CONNECTED)
                         .version(serverVersion)
                         .maxWireVersion(3)
                         .maxDocumentSize(16777216)
                         .electionId(electionId)
                         .setVersion(2)
                         .type(ServerType.REPLICA_SET_PRIMARY)
                         .setName('replset')
                         .primary('localhost:27017')
                         .canonicalAddress('localhost:27017')
                         .hosts(['localhost:27017', 'localhost:27018', 'localhost:27019'] as Set)
                         .arbiters(['localhost:27020'] as Set)
                         .tagSet(new TagSet([new Tag('dc', 'east'), new Tag('use', 'production')]))
                         .build()
    }

    def 'server description should reflect ismaster result from arbiter'() {
        expect:
        createServerDescription(serverAddress,
                parse('''{
                        "setName" : "replset",
                        "ismaster" : false,
                        "secondary" : false,
                        "hosts" : [
                        "localhost:27019",
                        "localhost:27018",
                        "localhost:27017"
                        ],
                        "arbiters" : [
                        "localhost:27020"
                        ],
                        "primary" : "localhost:27017",
                        "arbiterOnly" : true,
                        "me" : "localhost:27020",
                        "maxBsonObjectSize" : 16777216,
                        "maxMessageSizeBytes" : 48000000,
                        "maxWriteBatchSize" : 1000,
                        "localTime" : ISODate("2015-03-04T23:27:55.568Z"),
                        "maxWireVersion" : 3,
                        "minWireVersion" : 0,
                        "ok" : 1
                        }'''), serverVersion, roundTripTime) ==
        ServerDescription.builder()
                         .ok(true)
                         .address(serverAddress)
                         .state(ServerConnectionState.CONNECTED)
                         .version(serverVersion)
                         .maxWireVersion(3)
                         .maxDocumentSize(16777216)
                         .type(ServerType.REPLICA_SET_ARBITER)
                         .setName('replset')
                         .primary('localhost:27017')
                         .canonicalAddress('localhost:27020' )
                         .hosts(['localhost:27017', 'localhost:27018', 'localhost:27019'] as Set)
                         .arbiters(['localhost:27020'] as Set)
                         .build()
    }

    def 'server description should reflect ismaster result from other'() {
        given:
        def serverAddressOfHidden = new ServerAddress('localhost', 27020)

        when:
        def serverDescription = createServerDescription(serverAddressOfHidden,
                parse('''{
                        "setName" : "replset",
                        "ismaster" : false,
                        "secondary" : false,
                        "hosts" : [
                        "localhost:27019",
                        "localhost:27018",
                        "localhost:27017"
                        ],
                        "arbiters" : [
                        "localhost:27021"
                        ],
                        "primary" : "localhost:27017",
                        "arbiterOnly" : false,
                        "me" : "localhost:27020",
                        "maxBsonObjectSize" : 16777216,
                        "maxMessageSizeBytes" : 48000000,
                        "maxWriteBatchSize" : 1000,
                        "localTime" : ISODate("2015-03-04T23:27:55.568Z"),
                        "maxWireVersion" : 3,
                        "minWireVersion" : 0,
                        "ok" : 1
                        }'''), serverVersion, roundTripTime)

        then:
        serverDescription ==
        ServerDescription.builder()
                         .ok(true)
                         .address(serverAddressOfHidden)
                         .state(ServerConnectionState.CONNECTED)
                         .version(serverVersion)
                         .maxWireVersion(3)
                         .maxDocumentSize(16777216)
                         .type(ServerType.REPLICA_SET_OTHER)
                         .setName('replset')
                         .primary('localhost:27017')
                         .canonicalAddress('localhost:27020')
                         .hosts(['localhost:27017', 'localhost:27018', 'localhost:27019'] as Set)
                         .arbiters(['localhost:27021'] as Set)
                         .build()
    }

    def 'server description should reflect ismaster result from hidden'() {
        given:
        def serverAddressOfHidden = new ServerAddress('localhost', 27020)

        expect:
        createServerDescription(serverAddressOfHidden,
                parse('''{
                        "setName" : "replset",
                        "ismaster" : false,
                        "secondary" : true,
                        "hidden" : true,
                        "hosts" : [
                        "localhost:27019",
                        "localhost:27018",
                        "localhost:27017"
                        ],
                        "arbiters" : [
                        "localhost:27021"
                        ],
                        "primary" : "localhost:27017",
                        "arbiterOnly" : false,
                        "me" : "localhost:27020",
                        "maxBsonObjectSize" : 16777216,
                        "maxMessageSizeBytes" : 48000000,
                        "maxWriteBatchSize" : 1000,
                        "localTime" : ISODate("2015-03-04T23:27:55.568Z"),
                        "maxWireVersion" : 3,
                        "minWireVersion" : 0,
                        "ok" : 1
                        }'''), serverVersion, roundTripTime) ==
        ServerDescription.builder()
                         .ok(true)
                         .address(serverAddressOfHidden)
                         .state(ServerConnectionState.CONNECTED)
                         .version(serverVersion)
                         .maxWireVersion(3)
                         .maxDocumentSize(16777216)
                         .type(ServerType.REPLICA_SET_OTHER)
                         .setName('replset')
                         .primary('localhost:27017')
                         .canonicalAddress('localhost:27020')
                         .hosts(['localhost:27017', 'localhost:27018', 'localhost:27019'] as Set)
                         .arbiters(['localhost:27021'] as Set)
                         .build()
    }


    def 'server description should reflect ismaster result from ghost'() {
        expect:
        createServerDescription(serverAddress,
                parse('''{
                        "setName" : "replset",
                        "ismaster" : false,
                        "secondary" : false,
                        "arbiterOnly" : false,
                        "me" : "localhost:27020",
                        "maxBsonObjectSize" : 16777216,
                        "maxMessageSizeBytes" : 48000000,
                        "maxWriteBatchSize" : 1000,
                        "localTime" : ISODate("2015-03-04T23:27:55.568Z"),
                        "maxWireVersion" : 3,
                        "minWireVersion" : 0,
                        "ok" : 1
                        }'''), serverVersion, roundTripTime) ==
        ServerDescription.builder()
                         .ok(true)
                         .address(serverAddress)
                         .state(ServerConnectionState.CONNECTED)
                         .version(serverVersion)
                         .canonicalAddress('localhost:27020' )
                         .maxWireVersion(3)
                         .maxDocumentSize(16777216)
                         .type(ServerType.REPLICA_SET_GHOST)
                         .setName('replset')
                         .build()
    }

    def 'server description should reflect ismaster result from shard router'() {
        expect:
        createServerDescription(serverAddress,
                parse('''{
                        "ismaster" : true,
                        "msg" : "isdbgrid",
                        "maxBsonObjectSize" : 16777216,
                        "maxMessageSizeBytes" : 48000000,
                        "maxWriteBatchSize" : 1000,
                        "localTime" : ISODate("2015-03-04T23:55:18.505Z"),
                        "maxWireVersion" : 3,
                        "minWireVersion" : 0,
                        "ok" : 1
                        }'''), serverVersion, roundTripTime) ==
        ServerDescription.builder()
                         .ok(true)
                         .address(serverAddress)
                         .state(ServerConnectionState.CONNECTED)
                         .version(serverVersion)
                         .maxWireVersion(3)
                         .maxDocumentSize(16777216)
                         .type(ServerType.SHARD_ROUTER)
                         .build()
    }
}
