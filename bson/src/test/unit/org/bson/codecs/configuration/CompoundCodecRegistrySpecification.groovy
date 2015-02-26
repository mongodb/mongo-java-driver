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

import org.bson.codecs.MaxKeyCodec
import org.bson.codecs.MinKeyCodec
import org.bson.types.MaxKey
import org.bson.types.MinKey
import spock.lang.Specification

import static org.bson.codecs.configuration.CodecRegistryHelper.fromCodec

class CompoundCodecRegistrySpecification extends Specification {

    def 'should throw if supplied codec is null'() {
        given:
        def registry = fromCodec(new MinKeyCodec())

        when:
        new CompoundCodecRegistry(null, null)

        then:
        thrown(IllegalArgumentException)

        when:
        new CompoundCodecRegistry(registry, null)

        then:
        thrown(IllegalArgumentException)

        when:
        new CompoundCodecRegistry(null, registry)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should throw a CodecConfigurationException if codec not found'() {
        when:
        new CompoundCodecRegistry(fromCodec(new MinKeyCodec()), fromCodec(new MinKeyCodec())).get(MaxKey)

        then:
        thrown(CodecConfigurationException)
    }

    def 'should prefer the preferred codec registry'() {
        when:
        def codec1 = new MinKeyCodec()
        def codec2 = new MinKeyCodec()
        def codec3 = new MaxKeyCodec()
        def registry = new CompoundCodecRegistry(fromCodec(codec1), fromCodec(codec2))

        then:
        registry.get(MinKey) == codec1

        when:
        registry =  new CompoundCodecRegistry(fromCodec(codec3), fromCodec(codec2))

        then:
        registry.get(MinKey) == codec2
    }

    def 'get should use the codecCache'() {
        given:
        def codecRegistry1 = Mock(CodecRegistry) {
            1 * get(_) >> { throw new CodecConfigurationException('fail1') }
        }

        def codecRegistry2 = Mock(CodecRegistry) {
            1 * get(_) >> { throw new CodecConfigurationException('fail2') }
        }

        when:
        def registry = new CompoundCodecRegistry(codecRegistry1, codecRegistry2)
        registry.get(MinKey)

        then:
        thrown(CodecConfigurationException)

        when:
        registry.get(MinKey)

        then:
        thrown(CodecConfigurationException)
    }

}
