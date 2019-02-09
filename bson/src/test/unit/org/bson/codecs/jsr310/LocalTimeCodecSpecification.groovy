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

import java.time.LocalTime

class LocalTimeCodecSpecification extends JsrSpecification {

    def 'should round trip LocalTime successfully'() {
        when:
        def writer = encode(localTime)

        then:
        writer.getDocument().get('key').asDateTime().value == millis

        when:
        LocalTime actual = decode(writer)

        then:
        localTime == actual

        where:
        localTime                             | millis
        LocalTime.MIN                         | 0
        LocalTime.of(23, 59, 59, 999_000_000) | 86_399_999
    }

    def 'should round trip different timezones the same'() {
        given:
        def defaultTimeZone = TimeZone.getDefault()
        TimeZone.setDefault(TimeZone.getTimeZone(timeZone))
        def localDate = LocalTime.MIDNIGHT

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

    def 'should throw a CodecConfiguration exception if BsonType is invalid'() {
        when:
        decode(invalidDuration)

        then:
        thrown(CodecConfigurationException)

        where:
        invalidDuration << [
            BsonDocument.parse('{key: "10:00"}')
        ]
    }

    @Override
    Codec<?> getCodec() {
        new LocalTimeCodec()
    }
}
