/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.bson.codecs

import org.bson.BsonType
import org.bson.codecs.configuration.CodecConfigurationException
import spock.lang.Specification

import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries

class BsonTypeCodecMapSpecification extends Specification {
    def bsonTypeClassMap = new BsonTypeClassMap()
    def registry = fromRegistries(fromProviders(new DocumentCodecProvider(), new ValueCodecProvider(), new BsonValueCodecProvider()))
    def bsonTypeCodecMap = new BsonTypeCodecMap(bsonTypeClassMap, registry)

    def 'should map types to codecs'() {
        expect:
        bsonTypeCodecMap.get(BsonType.BINARY).class == BinaryCodec
        bsonTypeCodecMap.get(BsonType.BOOLEAN).class == BooleanCodec
        bsonTypeCodecMap.get(BsonType.DATE_TIME).class == DateCodec
        bsonTypeCodecMap.get(BsonType.DB_POINTER).class == BsonDBPointerCodec
        bsonTypeCodecMap.get(BsonType.DOCUMENT).class == DocumentCodec
        bsonTypeCodecMap.get(BsonType.DOUBLE).class == DoubleCodec
        bsonTypeCodecMap.get(BsonType.INT32).class == IntegerCodec
        bsonTypeCodecMap.get(BsonType.INT64).class == LongCodec
        bsonTypeCodecMap.get(BsonType.DECIMAL128).class == Decimal128Codec
        bsonTypeCodecMap.get(BsonType.MAX_KEY).class == MaxKeyCodec
        bsonTypeCodecMap.get(BsonType.MIN_KEY).class == MinKeyCodec
        bsonTypeCodecMap.get(BsonType.JAVASCRIPT).class == CodeCodec
        bsonTypeCodecMap.get(BsonType.JAVASCRIPT_WITH_SCOPE).class == CodeWithScopeCodec
        bsonTypeCodecMap.get(BsonType.OBJECT_ID).class == ObjectIdCodec
        bsonTypeCodecMap.get(BsonType.REGULAR_EXPRESSION).class == BsonRegularExpressionCodec
        bsonTypeCodecMap.get(BsonType.STRING).class == StringCodec
        bsonTypeCodecMap.get(BsonType.SYMBOL).class == SymbolCodec
        bsonTypeCodecMap.get(BsonType.TIMESTAMP).class == BsonTimestampCodec
        bsonTypeCodecMap.get(BsonType.UNDEFINED).class == BsonUndefinedCodec
    }

    def 'should throw exception for unmapped type'() {
        when:
        bsonTypeCodecMap.get(BsonType.NULL)

        then:
        thrown(CodecConfigurationException)
    }

    def 'should throw exception for unregistered codec'() {
        when:
        bsonTypeCodecMap.get(BsonType.ARRAY)

        then:
        thrown(CodecConfigurationException)
    }
}
