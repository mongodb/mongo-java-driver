/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.configuration

import org.bson.codecs.MinKeyCodec
import org.bson.types.MaxKey
import org.bson.types.MinKey
import spock.lang.Specification

class SingleCodecRegistrySpecification extends Specification {

    def 'should throw if supplied codec is null'() {
        when:
        new SingleCodecRegistry(null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should return the supplied codec for the codecs clazz'() {
        when:
        def codec = new MinKeyCodec()
        def registry = new SingleCodecRegistry(codec)

        then:
        registry.get(MinKey) == codec
    }

    def 'should throw a CodecConfigurationException if codec not found'() {
        when:
        new SingleCodecRegistry(new MinKeyCodec()).get(MaxKey)

        then:
        thrown(CodecConfigurationException)
    }
}
