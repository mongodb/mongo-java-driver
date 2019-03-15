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

package com.mongodb.operation

import com.mongodb.MongoException
import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcern
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.MongoCredential.createCredential
import static com.mongodb.MongoCredential.createScramSha1Credential
import static com.mongodb.MongoCredential.createScramSha256Credential

class DropUserOperationSpecification extends OperationFunctionalSpecification {

    @IgnoreIf({ !serverVersionAtLeast(3, 0) })
    def 'should delete user without error'() {
        given:
        def credential = createScramSha1Credential('userToDrop', databaseName, '123'.toCharArray())
        new CreateUserOperation(credential, true).execute(getBinding())
        DropUserOperation operation = new DropUserOperation(databaseName, credential.userName)

        when:
        execute(operation, async)

        then:
        notThrown(MongoException)

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(4, 0) })
    def 'should delete user without error SHA_256'() {
        given:
        def credential = createScramSha256Credential('userToDrop', databaseName, '123'.toCharArray())
        new CreateUserOperation(credential, true).execute(getBinding())
        DropUserOperation operation = new DropUserOperation(databaseName, credential.userName)

        when:
        execute(operation, async)

        then:
        notThrown(MongoException)

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) || !isDiscoverableReplicaSet() })
    def 'should throw MongoCommandException on write concern error'() {
        given:
        def credential = createCredential('userToDrop', databaseName, '123'.toCharArray())
        new CreateUserOperation(credential, true).execute(getBinding())
        def operation = new DropUserOperation(databaseName, credential.userName, new WriteConcern(5))

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.code == 100

        where:
        async << [true, false]
    }
}
