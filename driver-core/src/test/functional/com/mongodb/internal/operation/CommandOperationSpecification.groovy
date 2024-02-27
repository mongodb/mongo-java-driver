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

package com.mongodb.internal.operation


import com.mongodb.OperationFunctionalSpecification
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.codecs.BsonDocumentCodec
import util.spock.annotations.Slow

class CommandOperationSpecification extends OperationFunctionalSpecification {

    def 'should execute read command'() {
        given:
        def operation = new CommandReadOperation<BsonDocument>(getNamespace().databaseName,
                new BsonDocument('count', new BsonString(getCollectionName())),
                new BsonDocumentCodec())
        when:
        def result = execute(operation, async)

        then:
        result.getNumber('n').intValue() == 0


        where:
        async << [true, false]
    }


    @Slow
    def 'should execute command larger than 16MB'() {
        given:
        def operation = new CommandReadOperation<>(getNamespace().databaseName,
                new BsonDocument('findAndModify', new BsonString(getNamespace().fullName))
                        .append('query', new BsonDocument('_id', new BsonInt32(42)))
                        .append('update',
                                new BsonDocument('_id', new BsonInt32(42))
                                        .append('b', new BsonBinary(
                                                new byte[16 * 1024 * 1024 - 30]))),
                new BsonDocumentCodec())

        when:
        def result = execute(operation, async)

        then:
        result.containsKey('value')

        where:
        async << [true, false]
    }
}
