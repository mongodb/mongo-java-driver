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

package org.bson.codecs.jsr310

import org.bson.BsonDocument
import org.bson.codecs.Codec
import org.bson.codecs.configuration.CodecConfigurationException
import spock.lang.IgnoreIf

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

@IgnoreIf({ javaVersion < 1.8 })
class InstantCodecSpecification extends JsrSpecification {

    def 'should round trip Instant successfully'() {
        when:
        def writer = encode(instant)

        then:
        writer.getDocument().get('key').asDateTime().value == millis

        when:
        Instant actual = decode(writer)

        then:
        instant == actual

        where:
        instant                                                                             | millis
        Instant.EPOCH                                                             | 0
        LocalDateTime.of(2007, 10, 20, 0, 35).toInstant(ZoneOffset.UTC) | 1_192_840_500_000
    }

    def 'should wrap long overflow error in a CodecConfigurationException'() {
        when:
        encode(instant)

        then:
        def e = thrown(CodecConfigurationException)
        e.getCause().getClass() == ArithmeticException

        where:
        instant << [
            Instant.MIN,
            Instant.MAX
        ]
    }

    def 'should throw a CodecConfiguration exception if BsonType is invalid'() {
        when:
        decode(invalidDuration)

        then:
        thrown(CodecConfigurationException)

        where:
        invalidDuration << [
                BsonDocument.parse('{key: "10 Minutes"}'),
                BsonDocument.parse('{key: 10}')
        ]
    }

    @Override
    Codec<?> getCodec() {
        new InstantCodec()
    }
}
