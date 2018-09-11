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

package org.bson

import spock.lang.Specification
import spock.lang.Unroll

class BsonBinarySpecification extends Specification {

    @Unroll
    def 'should initialize with data'() {
        given:
        def bsonBinary = new BsonBinary((byte) 80, data as byte[])

        expect:
        data == bsonBinary.getData()

        where:
        data << [
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                [2, 5, 4, 67, 3, 4, 5, 2, 4, 2, 5, 6, 7, 4, 5, 12],
                [34, 24, 56, 76, 3, 4, 1, 12, 1, 9, 8, 7, 56, 46, 3, 9]
        ]
    }

    @Unroll
    def 'should initialize with data and BsonBinarySubType'() {
        given:
        byte[] data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        def bsonBinary = new BsonBinary(subType, data)

        expect:
        subType.getValue() == bsonBinary.getType()
        data == bsonBinary.getData()

        where:
        subType << [BsonBinarySubType.BINARY, BsonBinarySubType.FUNCTION, BsonBinarySubType.MD5,
                    BsonBinarySubType.OLD_BINARY, BsonBinarySubType.USER_DEFINED, BsonBinarySubType.UUID_LEGACY,
                    BsonBinarySubType.UUID_STANDARD]
    }

    @Unroll
    def 'should initialize with UUID'() {
        given:
        def bsonBinary = new BsonBinary(uuid)

        expect:
        uuid == bsonBinary.asUuid()

        where:
        uuid << [UUID.fromString('ffadee18-b533-11e8-96f8-529269fb1459'),
                 UUID.fromString('a5dc280e-b534-11e8-96f8-529269fb1459'),
                 UUID.fromString('4ef2a357-cb16-45a6-a6f6-a11ae1972917')]
    }

    @Unroll
    def 'should initialize with UUID and UUID representation'() {
        given:
        def uuid = UUID.fromString('ffadee18-b533-11e8-96f8-529269fb1459')
        def bsonBinary = new BsonBinary(uuid, uuidRepresentation)

        expect:
        uuid == bsonBinary.asUuid(uuidRepresentation)

        where:
        uuidRepresentation << [UuidRepresentation.STANDARD, UuidRepresentation.C_SHARP_LEGACY,
                               UuidRepresentation.JAVA_LEGACY, UuidRepresentation.PYTHON_LEGACY]
    }
}
