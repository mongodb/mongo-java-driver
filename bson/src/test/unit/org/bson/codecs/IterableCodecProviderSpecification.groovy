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

import spock.lang.Specification

import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class IterableCodecProviderSpecification extends Specification {

    def 'should provide codec for Iterables'() {
        given:
        def provider = new IterableCodecProvider()
        def registry = fromProviders(provider, new BsonValueCodecProvider(), new ValueCodecProvider(), new DocumentCodecProvider())

        expect:
        provider.get(Iterable, registry) instanceof IterableCodec
        provider.get(List, registry) instanceof IterableCodec
        provider.get(ArrayList, registry) instanceof IterableCodec
    }

    def 'should not provide codec for non-Iterables'() {
        given:
        def provider = new IterableCodecProvider()
        def registry = fromProviders(provider, new BsonValueCodecProvider(), new ValueCodecProvider(), new DocumentCodecProvider())

        expect:
        provider.get(Integer, registry) == null
    }

    def 'identical instances should be equal and have same hash code'() {
        given:
        def first = new IterableCodecProvider()
        def second = new IterableCodecProvider()

        expect:
        first.equals(first)
        first.equals(second)
        first.hashCode() == first.hashCode()
        first.hashCode() == second.hashCode()
    }

    def 'unidentical instances should not be equal'() {
        given:
        def first = new IterableCodecProvider()
        def second = new IterableCodecProvider(new BsonTypeClassMap([BOOLEAN: String]))
        def third = new IterableCodecProvider(new BsonTypeClassMap(), { Object from ->
            from
        })

        expect:
        !first.equals(Map)
        !first.equals(second)
        !first.equals(third)
        !second.equals(third)
    }
}