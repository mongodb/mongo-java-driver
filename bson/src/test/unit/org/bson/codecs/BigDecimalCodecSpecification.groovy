/*
 * Copyright 2017 MongoDB, Inc.
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

package org.bson.codecs

import org.bson.BsonDecimal128
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.BsonReader
import org.bson.types.Decimal128
import spock.lang.Specification

class BigDecimalCodecSpecification extends Specification {

    def 'should round trip BigDecimal successfully'() {
        given:
        def codec = new BigDecimalCodec()
        def bsonDecimal128 = new BsonDecimal128(new Decimal128(bigDecimal))

        when:
        def writer = new BsonDocumentWriter(new BsonDocument())
        writer.writeStartDocument()
        writer.writeName('bigDecimal')
        codec.encode(writer, bigDecimal, EncoderContext.builder().build())
        writer.writeEndDocument()

        then:
        bsonDecimal128 == writer.getDocument().get('bigDecimal')

        when:
        BsonReader bsonReader = new BsonDocumentReader(writer.getDocument())
        bsonReader.readStartDocument()
        bsonReader.readName()
        BigDecimal actual = codec.decode(bsonReader, DecoderContext.builder().build())

        then:
        bigDecimal == actual

        where:
        bigDecimal << [
                new BigDecimal(123),
                new BigDecimal(42L),
                new BigDecimal('12345678901234567890'),
                new BigDecimal(Long.valueOf(42)),
                new BigDecimal('42.0'),
                new BigDecimal(Double.valueOf(42)),
                new BigDecimal('1.2345678901234567890'),
                new BigDecimal(Long.MAX_VALUE),
                new BigDecimal(Long.MIN_VALUE),
                new BigDecimal(0),
        ]
    }
}
