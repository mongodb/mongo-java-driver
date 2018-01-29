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

import org.bson.BsonBinary
import org.bson.BsonBinarySubType
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.BsonInt32
import org.bson.BsonSymbol
import org.bson.BsonTimestamp
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.UuidCodec
import org.bson.codecs.ValueCodecProvider
import org.bson.types.BSONTimestamp
import org.bson.types.Binary
import org.bson.types.Symbol
import spock.lang.Specification

import java.sql.Timestamp

import static org.bson.UuidRepresentation.STANDARD
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries

class DBObjectCodecSpecification extends Specification {

    def bsonDoc = new BsonDocument()
    def codecRegistry = fromProviders([new ValueCodecProvider(), new DBObjectCodecProvider(), new BsonValueCodecProvider()])
    def dbObjectCodec = new DBObjectCodec(codecRegistry)

    def 'default registry should include necessary providers'() {
        when:
        def registry = DBObjectCodec.getDefaultRegistry()

        then:
        registry.get(Integer) != null
        registry.get(BsonInt32) != null
        registry.get(BSONTimestamp) != null
        registry.get(BasicDBObject) != null
    }

    def 'should encode with default registry'() {
        given:
        def document = new BsonDocument()
        def dBObject = new BasicDBObject('a', 0).append('b', new BsonInt32(1)).append('c', new BSONTimestamp())

        when:
         new DBObjectCodec().encode(new BsonDocumentWriter(document), dBObject, EncoderContext.builder().build())

        then:
        document == new BsonDocument('a', new BsonInt32(0)).append('b', new BsonInt32(1)).append('c', new BsonTimestamp())
    }

    def 'should encode and decode UUID as UUID'() {
        given:
        def uuid = UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10')
        def doc = new BasicDBObject('uuid', uuid)

        when:
        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build())

        then:
        bsonDoc.getBinary('uuid') == new BsonBinary(BsonBinarySubType.UUID_LEGACY,
                                                    [8, 7, 6, 5, 4, 3, 2, 1, 16, 15, 14, 13, 12, 11, 10, 9] as byte[])
        when:
        def decodedUuid = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build())

        then:
        decodedUuid.get('uuid') == uuid
    }

    def 'should decode a malformed UUID binary as Binary'() {
        given:
        def doc = new BasicDBObject('uuid', malformedBinaryUuid)
        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build())

        when:
        def decodedMalformedUuid = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build())

        then:
        decodedMalformedUuid.get('uuid') == malformedBinaryUuid

        where:
        malformedBinaryUuid << [new Binary(BsonBinarySubType.UUID_LEGACY,
                [8, 7, 6, 5, 4, 3, 2, 1, 16, 15, 14, 13, 12, 11, 10, 9, 8] as byte[]),
                                new Binary(BsonBinarySubType.UUID_STANDARD,
                                        [8, 7, 6, 5, 4, 3, 2, 1, 16, 15, 14, 13, 12, 11, 10] as byte[])]
    }

    def 'should encode and decode UUID as UUID with alternate UUID Codec'() {
        given:
        def codecWithAlternateUUIDCodec = new DBObjectCodec(fromRegistries(fromCodecs(new UuidCodec(STANDARD)), codecRegistry))
        def uuid = UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10')
        def doc = new BasicDBObject('uuid', uuid)

        when:
        codecWithAlternateUUIDCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build())

        then:
        bsonDoc.getBinary('uuid') == new BsonBinary(BsonBinarySubType.UUID_STANDARD,
                                                    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] as byte[])

        when:
        def decodedDoc = codecWithAlternateUUIDCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build())

        then:
        decodedDoc.get('uuid') == uuid
    }

    def 'should encode and decode byte array value as binary'() {
        given:
        def array = [0, 1, 2, 4, 4] as byte[]
        def doc = new BasicDBObject('byteArray', array)

        when:
        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build())

        then:
        bsonDoc.getBinary('byteArray') == new BsonBinary(array)

        when:
        DBObject decodedUuid = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build())

        then:
        decodedUuid.get('byteArray') == array
    }

    def 'should encode and decode Binary value as binary'() {
        given:
        def subType = (byte) 42
        def array = [0, 1, 2, 4, 4] as byte[]
        def doc = new BasicDBObject('byteArray', new Binary(subType, array))

        when:
        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build())

        then:
        bsonDoc.getBinary('byteArray') == new BsonBinary(subType, array)

        when:
        DBObject decodedUuid = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build())

        then:
        decodedUuid.get('byteArray') == new Binary(subType, array)
    }

    def 'should encode Symbol to BsonSymbol and decode BsonSymbol to String'() {
        given:
        def symbol = new Symbol('symbol');
        def doc = new BasicDBObject('symbol', symbol)

        when:
        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build())

        then:
        bsonDoc.get('symbol') == new BsonSymbol('symbol')

        when:
        def decodedSymbol = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build())

        then:
        decodedSymbol.get('symbol') == symbol.toString()
    }

    def 'should encode java.sql.Date as date'() {
        given:
        def sqlDate = new java.sql.Date(System.currentTimeMillis())
        def doc = new BasicDBObject('d', sqlDate)

        when:
        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build())

        then:
        def decodededDoc = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build())

        then:
        ((Date) decodededDoc.get('d')).getTime() == sqlDate.getTime()
    }

    def 'should encode java.sql.Timestamp as date'() {
        given:
        def sqlTimestamp = new Timestamp(System.currentTimeMillis())
        def doc = new BasicDBObject('d', sqlTimestamp)

        when:
        dbObjectCodec.encode(new BsonDocumentWriter(bsonDoc), doc, EncoderContext.builder().build())

        then:
        def decodededDoc = dbObjectCodec.decode(new BsonDocumentReader(bsonDoc), DecoderContext.builder().build())

        then:
        ((Date) decodededDoc.get('d')).getTime() == sqlTimestamp.getTime()
    }
}
