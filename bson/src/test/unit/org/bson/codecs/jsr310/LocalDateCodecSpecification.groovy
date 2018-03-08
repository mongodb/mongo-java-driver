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

import java.time.LocalDate

@IgnoreIf({ javaVersion < 1.8 })
class LocalDateCodecSpecification extends JsrSpecification {

    def 'should round trip LocalDate successfully'() {
        when:
        def writer = encode(localDate)

        then:
        writer.getDocument().get('key').asDateTime().value == millis

        when:
        LocalDate actual = decode(writer)

        then:
        localDate == actual

        where:
        localDate                             | millis
        LocalDate.of(2007, 10, 20)            | 1_192_838_400_000
        LocalDate.ofEpochDay(0)               | 0
        LocalDate.ofEpochDay(-99_999_999_999) | -99_999_999_999 * 86_400_000
        LocalDate.ofEpochDay(99_999_999_999)  | 99_999_999_999 * 86_400_000
    }

    def 'should round trip different timezones the same'() {
        given:
        def defaultTimeZone = TimeZone.getDefault()
        TimeZone.setDefault(TimeZone.getTimeZone(timeZone))
        def localDate = LocalDate.ofEpochDay(0)

        when:
        def writer = encode(localDate)

        then:
        writer.getDocument().get('key').asDateTime().value == 0

        when:
        def actual = decode(writer)

        then:
        localDate == actual

        cleanup:
        TimeZone.setDefault(defaultTimeZone)

        where:
        timeZone << ['Pacific/Auckland', 'UTC', 'US/Hawaii']
    }

    def 'should wrap long overflow error in a CodecConfigurationException'() {
        when:
        encode(localDate)

        then:
        def e = thrown(CodecConfigurationException)
        e.getCause().getClass() == ArithmeticException

        where:
        localDate << [
                LocalDate.MIN,
                LocalDate.MAX
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
        new LocalDateCodec()
    }
}
