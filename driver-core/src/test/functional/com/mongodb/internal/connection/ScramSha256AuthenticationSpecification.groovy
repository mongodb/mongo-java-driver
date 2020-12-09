/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.connection

import com.mongodb.ClusterFixture
import com.mongodb.MongoCredential
import com.mongodb.MongoSecurityException
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.async.FutureResultCallback
import com.mongodb.internal.binding.AsyncClusterBinding
import com.mongodb.internal.binding.ClusterBinding
import com.mongodb.internal.operation.CommandReadOperation
import org.bson.BsonDocument
import org.bson.BsonDocumentWrapper
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf
import spock.lang.Specification

import static com.mongodb.ClusterFixture.createAsyncCluster
import static com.mongodb.ClusterFixture.createCluster
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isAuthenticated
import static com.mongodb.MongoCredential.createCredential
import static com.mongodb.MongoCredential.createScramSha1Credential
import static com.mongodb.MongoCredential.createScramSha256Credential

@IgnoreIf({ !ClusterFixture.serverVersionAtLeast(4, 0) || !isAuthenticated() })
class ScramSha256AuthenticationSpecification extends Specification {

    static MongoCredential sha1Implicit = createCredential('sha1', 'admin', 'sha1'.toCharArray())
    static MongoCredential sha1Explicit = createScramSha1Credential('sha1', 'admin', 'sha1'.toCharArray())
    static MongoCredential sha256Implicit = createCredential('sha256', 'admin', 'sha256'.toCharArray())
    static MongoCredential sha256Explicit = createScramSha256Credential('sha256', 'admin', 'sha256'.toCharArray())
    static MongoCredential bothImplicit = createCredential('both', 'admin', 'both'.toCharArray())
    static MongoCredential bothExplicitSha1 = createScramSha1Credential('both', 'admin', 'both'.toCharArray())
    static MongoCredential bothExplicitSha256 = createScramSha256Credential('both', 'admin', 'both'.toCharArray())

    static MongoCredential sha1AsSha256 = createScramSha256Credential('sha1', 'admin', 'sha1'.toCharArray())
    static MongoCredential sha256AsSha1 = createScramSha1Credential('sha256', 'admin', 'sha256'.toCharArray())
    static MongoCredential nonExistentUserImplicit = createCredential('nonexistent', 'admin', 'pwd'.toCharArray())

    static MongoCredential userNinePrepped = createScramSha256Credential('IX', 'admin', 'IX'.toCharArray())
    static MongoCredential userNineUnprepped = createScramSha256Credential('IX', 'admin', 'I\u00ADX'.toCharArray())

    static MongoCredential userFourPrepped = createScramSha256Credential('\u2168', 'admin', 'IV'.toCharArray())
    static MongoCredential userFourUnprepped = createScramSha256Credential('\u2168', 'admin', 'I\u00ADV'.toCharArray())

    def setupSpec() {
        createUser('sha1', 'sha1', ['SCRAM-SHA-1'])
        createUser('sha256', 'sha256', ['SCRAM-SHA-256'])
        createUser('both', 'both', ['SCRAM-SHA-1', 'SCRAM-SHA-256'])
        createUser('IX', 'IX', ['SCRAM-SHA-256'])
        createUser('\u2168', '\u2163', ['SCRAM-SHA-256'])
    }


    def cleanupSpec() {
        dropUser('sha1')
        dropUser('sha256')
        dropUser('both')
        dropUser('IX')
        dropUser('\u2168')
    }

    def createUser(final String userName, final String password, final List<String> mechanisms) {
        def createUserCommand = new Document('createUser', userName)
                .append('pwd', password)
                .append('roles', ['root'])
                .append('mechanisms', mechanisms)
        new CommandReadOperation<>('admin',
                new BsonDocumentWrapper<Document>(createUserCommand, new DocumentCodec()), new DocumentCodec())
                .execute(getBinding())
    }

    def dropUser(final String userName) {
        new CommandReadOperation<>('admin', new BsonDocument('dropUser', new BsonString(userName)),
            new BsonDocumentCodec()).execute(getBinding())
    }

    def 'test authentication and authorization'() {
        given:
        def cluster = createCluster(credential)

        when:
        new CommandReadOperation<Document>('admin',
                new BsonDocumentWrapper<Document>(new Document('dbstats', 1), new DocumentCodec()), new DocumentCodec())
                .execute(new ClusterBinding(cluster, ReadPreference.primary(), ReadConcern.DEFAULT, null))

        then:
        noExceptionThrown()

        cleanup:
        cluster.close()

        where:
        credential << [sha1Implicit, sha1Explicit, sha256Implicit, sha256Explicit, bothImplicit, bothExplicitSha1, bothExplicitSha256]
    }

    def 'test authentication and authorization async'() {
        given:
        def cluster = createAsyncCluster(credential)
        def callback = new FutureResultCallback()

        when:
        // make this synchronous
        new CommandReadOperation<Document>('admin',
                new BsonDocumentWrapper<Document>(new Document('dbstats', 1), new DocumentCodec()), new DocumentCodec())
                .executeAsync(new AsyncClusterBinding(cluster, ReadPreference.primary(), ReadConcern.DEFAULT, null), callback)
        callback.get()

        then:
        noExceptionThrown()

        cleanup:
        cluster.close()

        where:
        credential << [sha1Implicit, sha1Explicit, sha256Implicit, sha256Explicit, bothImplicit, bothExplicitSha1, bothExplicitSha256]
    }

    def 'test authentication and authorization failure with wrong mechanism'() {
        given:
        def cluster = createCluster(credential)

        when:
        new CommandReadOperation<Document>('admin',
                new BsonDocumentWrapper<Document>(new Document('dbstats', 1), new DocumentCodec()), new DocumentCodec())
                .execute(new ClusterBinding(cluster, ReadPreference.primary(), ReadConcern.DEFAULT, null))

        then:
        thrown(MongoSecurityException)

        cleanup:
        cluster.close()

        where:
        credential << [sha1AsSha256, sha256AsSha1, nonExistentUserImplicit]
    }

    def 'test authentication and authorization failure with wrong mechanism async'() {
        given:
        def cluster = createAsyncCluster(credential)
        def callback = new FutureResultCallback()

        when:
        new CommandReadOperation<Document>('admin',
                new BsonDocumentWrapper<Document>(new Document('dbstats', 1), new DocumentCodec()), new DocumentCodec())
                .executeAsync(new AsyncClusterBinding(cluster, ReadPreference.primary(), ReadConcern.DEFAULT, null), callback)
        callback.get()

        then:
        thrown(MongoSecurityException)

        cleanup:
        cluster.close()

        where:
        credential << [sha1AsSha256, sha256AsSha1, nonExistentUserImplicit]
    }

    def 'test SASL Prep'() {
        given:
        def cluster = createCluster(credential)

        when:
        new CommandReadOperation<Document>('admin',
                new BsonDocumentWrapper<Document>(new Document('dbstats', 1), new DocumentCodec()), new DocumentCodec())
                .execute(new ClusterBinding(cluster, ReadPreference.primary(), ReadConcern.DEFAULT, null))

        then:
        noExceptionThrown()

        cleanup:
        cluster.close()

        where:
        credential << [userNinePrepped, userNineUnprepped, userFourPrepped, userFourUnprepped]
    }

    def 'test SASL Prep async'() {
        given:
        def cluster = createAsyncCluster(credential)
        def callback = new FutureResultCallback()

        when:
        new CommandReadOperation<Document>('admin',
                new BsonDocumentWrapper<Document>(new Document('dbstats', 1), new DocumentCodec()), new DocumentCodec())
                .executeAsync(new AsyncClusterBinding(cluster, ReadPreference.primary(), ReadConcern.DEFAULT, null), callback)
        callback.get()

        then:
        noExceptionThrown()

        cleanup:
        cluster.close()

        where:
        credential << [userNinePrepped, userNineUnprepped, userFourPrepped, userFourUnprepped]
    }
}
