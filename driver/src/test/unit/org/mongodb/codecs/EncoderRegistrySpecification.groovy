/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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



package org.mongodb.codecs

import org.bson.types.CodeWithScope
import org.mongodb.DBRef
import org.mongodb.Encoder
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Subject

class EncoderRegistrySpecification extends Specification {
    @Subject
    private final EncoderRegistry encoderRegistry = new EncoderRegistry()

    @Ignore('arrays are special....')
    def 'should return ArrayCodec as default for class that is an array'() {
        when:
        int[] intArray = new int[0];
        Encoder encoder = encoderRegistry.get(intArray.getClass());

        then:
        encoder instanceof ArrayCodec;
    }

    @Ignore('map codec is not actually a codec yet...')
    def 'should return MapCodec as default for class that is a Map'() {
        when:
        Encoder encoder = encoderRegistry.get([:].getClass());

        then:
        encoder instanceof MapCodec;
    }

    def 'should return IterableCodec as default for class that is iterable'() {
        when:
        Encoder encoder = encoderRegistry.get(ArrayList);

        then:
        encoder instanceof IterableCodec;
    }

    def 'should return CodeWithScopeCodec as default for class that is CodeWithScope'() {
        when:
        Encoder encoder = encoderRegistry.get(CodeWithScope);

        then:
        encoder instanceof CodeWithScopeCodec;
    }

    def 'should return DBRefCodec as default for class that is DBRef'() {
        when:
        Encoder encoder = encoderRegistry.get(DBRef);

        then:
        encoder instanceof DBRefEncoder;
    }

    def 'should return DocumentCodec by default'() {
        when:
        Encoder encoder = encoderRegistry.getDefaultEncoder()

        then:
        encoder instanceof DocumentCodec;
    }

    def 'should be able to override the default codec for Objects'() {
        when:
        encoderRegistry.register(Object, new PojoCodec<Object>(Codecs.createDefault(), SomeObject))
        Encoder encoder = encoderRegistry.get(SomeObject);

        then:
        encoder instanceof PojoCodec;
    }

    private class SomeObject { }
}