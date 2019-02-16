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
package org.bson.codecs.pojo;

import org.bson.*;
import org.bson.codecs.*;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.entities.SimpleEnum;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.bson.io.OutputBuffer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.lang.String.format;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.pojo.PojoTestCase.getPojoCodecProviderBuilder;

@RunWith(Parameterized.class)
public class EnumRoundTripTest {

    private final String name;
    private final Enum<?> enumValue;
    private final BsonValue encodedEnum;
    private final PojoCodecProvider.Builder builder;

    public EnumRoundTripTest(String name, Enum<?> enumValue, BsonValue encodedEnum, PojoCodecProvider.Builder builder) {
        this.name = name;
        this.builder = builder;
        this.enumValue = enumValue;
        this.encodedEnum = encodedEnum;
    }

    @Test
    public void test() {
        roundTrip(builder, enumValue, encodedEnum);
    }

    private static List<TestData> testCases() {
        List<TestData> data = new ArrayList<>(1);
        data.add(new TestData("Enum Support",
                SimpleEnum.BRAVO,
                getPojoCodecProviderBuilder(SimpleEnum.class),
                "BRAVO"));
        return data;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> data = new ArrayList<Object[]>();

        for (TestData testData : testCases()) {
            data.add(new Object[]{format("%s", testData.getName()), testData.getEnumValue(), testData.getEncodedEnum(), testData.getBuilder()});
            data.add(new Object[]{format("%s [Auto]", testData.getName()), testData.getEnumValue(), testData.getEncodedEnum(), AUTOMATIC_BUILDER});
            data.add(new Object[]{format("%s [Package]", testData.getName()), testData.getEnumValue(), testData.getEncodedEnum(), PACKAGE_BUILDER});
        }
        return data;
    }

    private static final PojoCodecProvider.Builder AUTOMATIC_BUILDER = PojoCodecProvider.builder().automatic(true);
    private static final PojoCodecProvider.Builder PACKAGE_BUILDER = PojoCodecProvider.builder().register("org.bson.codecs.pojo.entities",
            "org.bson.codecs.pojo.entities.conventions");


    private static class TestData {
        private final String name;
        private final Enum<?> enumValue;
        private final BsonValue encodedEnum;
        private final PojoCodecProvider.Builder builder;

        TestData(final String name, final Enum<?> enumValue, final PojoCodecProvider.Builder builder, final String encodedEnum) {
            this.name = name;
            this.enumValue = enumValue;
            this.builder = builder;
            this.encodedEnum = new BsonString(encodedEnum);
        }

        TestData(final String name, final Enum<?> enumValue, final PojoCodecProvider.Builder builder, final Integer encodedEnum) {
            this.name = name;
            this.enumValue = enumValue;
            this.builder = builder;
            this.encodedEnum = new BsonInt32(encodedEnum);
        }

        public String getName() {
            return name;
        }

        public Enum<?> getEnumValue() {
            return enumValue;
        }

        public BsonValue getEncodedEnum() {
            return encodedEnum;
        }

        public PojoCodecProvider.Builder getBuilder() {
            return builder;
        }
    }

    private void roundTrip(PojoCodecProvider.Builder builder, Enum<?> enumValue, BsonValue encodedEnum) {
        encodesTo(getCodecRegistry(builder), enumValue, encodedEnum);
        decodesTo(getCodecRegistry(builder), encodedEnum, enumValue);
    }

    static final BsonValueCodec VALUE_CODEC = new BsonValueCodec();

    CodecRegistry getCodecRegistry(final PojoCodecProvider.Builder builder) {
        return fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider(), builder.build());
    }

    private <E extends Enum<?>> void encodesTo(final CodecRegistry registry, final E enumValue, final BsonValue encodedEnum) {
        @SuppressWarnings("unchecked") Codec<E> codec = (Codec<E>) registry.get(enumValue.getDeclaringClass());

        OutputBuffer encodedActual = encode(codec, enumValue);
        Object actualAsValue = decode(VALUE_CODEC, encodedActual);
        Assert.assertEquals("Encoded Value", encodedEnum, actualAsValue);
    }

    private <E extends Enum<?>> void decodesTo(CodecRegistry registry, BsonValue encodedEnum, E enumValue) {
        @SuppressWarnings("unchecked") Codec<E> codec = (Codec<E>) registry.get(enumValue.getDeclaringClass());

        OutputBuffer toDecode = encode(VALUE_CODEC, encodedEnum);
        E actual = decode(codec, toDecode);
        Assert.assertEquals("Decoded value", enumValue, actual);
    }

    <T> OutputBuffer encode(final Codec<T> codec, final T value) {
        OutputBuffer buffer = new BasicOutputBuffer();
        BsonWriter writer = new BsonBinaryWriter(buffer);
        writer.writeStartDocument();
        writer.writeName("value");
        codec.encode(writer, value, EncoderContext.builder().isEncodingCollectibleDocument(false).build());
        writer.writeEndDocument();
        return buffer;
    }

    <T> T decode(final Codec<T> codec, final OutputBuffer buffer) {
        BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray()))));
        reader.readStartDocument();
        reader.readName("value");
        return codec.decode(reader, DecoderContext.builder().build());
    }
}
