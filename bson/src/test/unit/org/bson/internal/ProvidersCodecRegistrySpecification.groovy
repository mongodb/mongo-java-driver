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

package org.bson.internal

import org.bson.BsonBinaryReader
import org.bson.BsonBinaryWriter
import org.bson.BsonReader
import org.bson.BsonType
import org.bson.BsonWriter
import org.bson.ByteBufNIO
import org.bson.codecs.Codec
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.codecs.MinKeyCodec
import org.bson.codecs.configuration.CodecConfigurationException
import org.bson.codecs.configuration.CodecProvider
import org.bson.codecs.configuration.CodecRegistry
import org.bson.io.BasicOutputBuffer
import org.bson.io.ByteBufferBsonInput
import org.bson.types.MaxKey
import org.bson.types.MinKey
import spock.lang.Specification

import java.nio.ByteBuffer

import static java.util.Arrays.asList

class ProvidersCodecRegistrySpecification extends Specification {

    def 'should throw if supplied codecProviders is null or an empty list'() {
        when:
        new ProvidersCodecRegistry(null)

        then:
        thrown(IllegalArgumentException)

        when:
        new ProvidersCodecRegistry([])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should throw a CodecConfigurationException if codec not found'() {
        when:
        new ProvidersCodecRegistry([new SingleCodecProvider(new MinKeyCodec())]).get(MaxKey)

        then:
        thrown(CodecConfigurationException)
    }

    def 'get should return registered codec'() {
        given:
        def minKeyCodec = new MinKeyCodec()
        def registry = new ProvidersCodecRegistry([new SingleCodecProvider(minKeyCodec)])

        expect:
        registry.get(MinKey).is(minKeyCodec)
    }

    def 'get should return the codec from the first source that has one'() {
        given:
        def minKeyCodec1 = new MinKeyCodec()
        def minKeyCodec2 = new MinKeyCodec()
        def registry = new ProvidersCodecRegistry([new SingleCodecProvider(minKeyCodec1), new SingleCodecProvider(minKeyCodec2)])

        expect:
        registry.get(MinKey).is(minKeyCodec1)
    }

    def 'should handle cycles'() {
        given:
        def registry = new ProvidersCodecRegistry([new ClassModelCodecProvider()])

        when:
        Codec<Top> topCodec = registry.get(Top)

        then:
        topCodec instanceof TopCodec

        when:
        def top = new Top('Bob',
                          new Top('Jim', null, null),
                          new Nested('George', new Top('Joe', null, null)))
        def writer = new BsonBinaryWriter(new BasicOutputBuffer())
        topCodec.encode(writer, top, EncoderContext.builder().build())
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        writer.getBsonOutput().pipe(os);
        writer.close()

        then:
        topCodec.decode(new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(os.toByteArray())))),
                        DecoderContext.builder().build()) == top
    }

    def 'get should use the codecCache'() {
        given:
        def provider = Mock(CodecProvider)

        when:
        def registry = new ProvidersCodecRegistry([provider])
        registry.get(MinKey)

        then:
        thrown(CodecConfigurationException)
        1 * provider.get(MinKey, _)

        when:
        registry.get(MinKey)

        then:
        thrown(CodecConfigurationException)
        0 * provider.get(MinKey, _)
    }

    def 'get with codec registry should return the codec from the first source that has one'() {
        given:
        def provider = new ProvidersCodecRegistry([new ClassModelCodecProvider([Simple])])
        def registry = Mock(CodecRegistry)

        expect:
        provider.get(Simple, registry) instanceof SimpleCodec
    }

    def 'get with codec registry should return null if codec not found'() {
        given:
        def provider = new ProvidersCodecRegistry([new ClassModelCodecProvider([Top])])
        def registry = Mock(CodecRegistry)

        expect:
        !provider.get(Simple, registry)
    }

    def 'get with codec registry should pass the outer registry to its providers'() {
        given:
        def provider = new ProvidersCodecRegistry([new ClassModelCodecProvider([Simple])])
        def registry = Mock(CodecRegistry)

        expect:
        ((SimpleCodec) provider.get(Simple, registry)).registry.is(registry)
    }
}

class SingleCodecProvider implements CodecProvider {

    private final Codec<?> codec

    SingleCodecProvider(final Codec<?> codec) {
        this.codec = codec
    }

    @Override
    def <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (clazz == codec.getEncoderClass()) {
            return codec
        }
        null
    }
}

class ClassModelCodecProvider implements CodecProvider {

    private final List<Class<?>> supportedClasses

    ClassModelCodecProvider() {
        this(asList(Top, Nested))
    }

    ClassModelCodecProvider(List<Class<?>> supportedClasses) {
        this.supportedClasses = supportedClasses
    }

    @Override
    @SuppressWarnings('ReturnNullFromCatchBlock')
    def <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (!supportedClasses.contains(clazz)) {
            null
        } else if (clazz == Top) {
            try {
                new TopCodec(registry)
            } catch (CodecConfigurationException e) {
                null
            }
        } else if (clazz == Nested) {
            try {
                new NestedCodec(registry)
            } catch (CodecConfigurationException e) {
                null
            }
        } else if (clazz == Simple) {
            new SimpleCodec(registry)
        } else {
            null
        }
    }
}

class TopCodec implements Codec<Top> {
    Codec<TopCodec> codecForOther
    Codec<Nested> codecForNested
    CodecRegistry registry

    TopCodec(final CodecRegistry registry) {
        this.registry = registry
        codecForOther = registry.get(Top)
        codecForNested = registry.get(Nested)
    }

    @Override
    void encode(final BsonWriter writer, final Top top, EncoderContext encoderContext) {
        if (top == null) {
            writer.writeNull()
            return
        }

        writer.writeStartDocument()
        writer.writeString('name', top.getName())
        writer.writeName('other')
        codecForOther.encode(writer, top.getOther(), EncoderContext.builder().build())
        writer.writeName('nested')
        codecForNested.encode(writer, top.getNested(), EncoderContext.builder().build())
        writer.writeEndDocument()
    }

    @Override
    Class<Top> getEncoderClass() {
        Top
    }

    @Override
    Top decode(final BsonReader reader, DecoderContext decoderContext) {
        reader.readStartDocument()
        reader.readName()
        def name = reader.readString()
        def other = null
        def nested = null

        def type = reader.readBsonType()

        reader.readName()
        if (type == BsonType.NULL) {
            reader.readNull()
        } else {
            other = codecForOther.decode(reader, decoderContext)
        }

        reader.readName('nested')
        if (type == BsonType.NULL) {
            reader.readNull()
        } else {
            nested = codecForNested.decode(reader, decoderContext)
        }
        reader.readEndDocument()
        new Top(name, other, nested);
    }
}

class NestedCodec implements Codec<Nested> {
    Codec<TopCodec> codecForTop

    NestedCodec(final CodecRegistry registry) {
        codecForTop = registry.get(Top)
    }

    @Override
    void encode(final BsonWriter writer, final Nested nested, EncoderContext encoderContext) {
        if (nested == null) {
            writer.writeNull()
            return
        }

        writer.writeStartDocument()
        writer.writeString('name', nested.getName())
        writer.writeName('top')
        codecForTop.encode(writer, nested.getTop(), EncoderContext.builder().build())
        writer.writeEndDocument()
    }

    @Override
    Class<Top> getEncoderClass() {
        Top
    }

    @Override
    Nested decode(final BsonReader reader, DecoderContext decoderContext) {
        reader.readStartDocument()
        reader.readName()
        def name = reader.readString()
        def type = reader.readBsonType()
        reader.readName()
        def top = null
        if (type == BsonType.NULL) {
            reader.readNull()
        } else {
            top = codecForTop.decode(reader, decoderContext)
        }
        reader.readEndDocument()
        new Nested(name, top);
    }
}

class SimpleCodec implements Codec<Simple> {
    private final CodecRegistry registry

    SimpleCodec(CodecRegistry registry) {
        this.registry = registry
    }

    CodecRegistry getRegistry() {
        registry
    }

    @Override
    void encode(final BsonWriter writer, final Simple value, final EncoderContext encoderContext) {
        writer.writeNull()
    }

    @Override
    Class<Simple> getEncoderClass() {
        Simple
    }

    @Override
    Simple decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readNull()
        new Simple()
    }
}

class Top {
    private String name
    private Top other
    private Nested nested

    Top(final String name, final Top other, final Nested nested) {
        this.name = name
        this.other = other
        this.nested = nested
    }

    String getName() {
        name
    }

    Top getOther() {
        other
    }

    Nested getNested() {
        nested
    }

    boolean equals(final o) {
        if (this.is(o)) {
            return true
        }
        if (getClass() != o.class) {
            return false
        }

        Top top = (Top) o

        if (name != top.name) {
            return false
        }
        if (nested != top.nested) {
            return false
        }
        if (other != top.other) {
            return false
        }

        true
    }

    int hashCode() {
        int result
        result = name.hashCode()
        result = 31 * result + (other != null ? other.hashCode() : 0)
        result = 31 * result + (nested != null ? nested.hashCode() : 0)
        result
    }
}

class Nested {
    private String name
    private Top top

    Nested(final String name, final Top top) {
        this.name = name
        this.top = top
    }

    String getName() {
        name
    }

    Top getTop() {
        top
    }

    boolean equals(final o) {
        if (this.is(o)) {
            return true
        }
        if (getClass() != o.class) {
            return false
        }

        Nested nested = (Nested) o

        if (name != nested.name) {
            return false
        }
        if (top != nested.top) {
            return false
        }

        true
    }

    int hashCode() {
        int result
        result = name.hashCode()
        result = 31 * result + (top != null ? top.hashCode() : 0)
        result
    }
}

class Simple {
    int value = 0
}

