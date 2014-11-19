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

package com.mongodb.operation

import com.mongodb.MongoException
import com.mongodb.OperationFunctionalSpecification

import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.MongoCredential.createMongoCRCredential

class DropUserOperationSpecification extends OperationFunctionalSpecification {
    def 'should delete user without error'() {
        given:
        def credential = createMongoCRCredential('userToDrop', databaseName, '123'.toCharArray())
        new CreateUserOperation(credential, true).execute(getBinding())

        when:
        DropUserOperation operation = new DropUserOperation(databaseName, credential.userName)
        operation.execute(getBinding())

        then:
        notThrown(MongoException)
    }

}
