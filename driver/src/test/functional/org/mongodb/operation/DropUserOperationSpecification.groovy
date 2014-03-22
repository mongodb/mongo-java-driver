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



package org.mongodb.operation

import org.mongodb.FunctionalSpecification
import org.mongodb.MongoException

import static org.mongodb.Fixture.session
import static org.mongodb.MongoCredential.createMongoCRCredential

class DropUserOperationSpecification extends FunctionalSpecification {
    def 'should delete user without error'() {
        given:
        User jeff = new User(createMongoCRCredential('jeff', databaseName, '123'.toCharArray()), true)
        new CreateUserOperation(jeff, session, true).execute()

        when:
        DropUserOperation operation = new DropUserOperation(databaseName, 'jeff', session, true)
        operation.execute()

        then:
        notThrown(MongoException)
    }

}
