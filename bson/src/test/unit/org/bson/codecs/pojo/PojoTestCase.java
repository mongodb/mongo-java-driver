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

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.ByteBufNIO;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.EnumCodecProvider;
import org.bson.codecs.SimpleEnum;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.jsr310.Jsr310CodecProvider;
import org.bson.codecs.pojo.entities.CollectionNestedPojoModel;
import org.bson.codecs.pojo.entities.ConcreteCollectionsModel;
import org.bson.codecs.pojo.entities.ConventionModel;
import org.bson.codecs.pojo.entities.GenericHolderModel;
import org.bson.codecs.pojo.entities.GenericTreeModel;
import org.bson.codecs.pojo.entities.InvalidMapModel;
import org.bson.codecs.pojo.entities.MultipleBoundsModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderFieldWithMultipleTypeParamsModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderMapModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderSimpleGenericsModel;
import org.bson.codecs.pojo.entities.NestedReusedGenericsModel;
import org.bson.codecs.pojo.entities.NestedSelfReferentialGenericHolderModel;
import org.bson.codecs.pojo.entities.NestedSelfReferentialGenericModel;
import org.bson.codecs.pojo.entities.PrimitivesModel;
import org.bson.codecs.pojo.entities.PropertyWithMultipleTypeParamsModel;
import org.bson.codecs.pojo.entities.ReusedGenericsModel;
import org.bson.codecs.pojo.entities.SelfReferentialGenericModel;
import org.bson.codecs.pojo.entities.ShapeModelCircle;
import org.bson.codecs.pojo.entities.ShapeModelRectangle;
import org.bson.codecs.pojo.entities.SimpleGenericsModel;
import org.bson.codecs.pojo.entities.SimpleModel;
import org.bson.codecs.pojo.entities.SimpleNestedPojoModel;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.bson.io.OutputBuffer;
import org.bson.types.ObjectId;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.pojo.Conventions.DEFAULT_CONVENTIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

abstract class PojoTestCase {

    static final BsonDocumentCodec DOCUMENT_CODEC = new BsonDocumentCodec();

    <T> void roundTrip(final T value, final String json) {
        roundTrip(PojoCodecProvider.builder().automatic(true), value, json);
    }

    <T> void roundTrip(final PojoCodecProvider.Builder builder, final T value, final String json) {
        encodesTo(getCodecRegistry(builder), value, json);
        decodesTo(getCodecRegistry(builder), json, value);
    }

    <T> void threadedRoundTrip(final PojoCodecProvider.Builder builder, final T value, final String json) {
        int numberOfThreads = 5;
        ExecutorService service = null;
        try {
            service = Executors.newFixedThreadPool(10);
            CountDownLatch latch = new CountDownLatch(numberOfThreads);
            List<String> errors = new ArrayList<>();
            CodecRegistry codecRegistry = getCodecRegistry(builder);
            for (int i = 0; i < numberOfThreads; i++) {
                service.submit(() -> {
                    try {
                        encodesTo(codecRegistry, value, json);
                        decodesTo(codecRegistry, json, value);
                    } catch (Exception e) {
                        errors.add(e instanceof  NullPointerException ? "NPE: " + e.getStackTrace()[0] : e.getMessage());
                    }
                    latch.countDown();
                });
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                // Ignore
            }
            assertTrue(format("Errors encountered: [%s]", String.join(",", errors)), errors.isEmpty());
        } finally {
            if (service != null) {
                service.shutdown();
            }
        }
    }

    <T> void roundTrip(final CodecRegistry registry, final T value, final String json) {
        encodesTo(registry, value, json);
        decodesTo(registry, json, value);
    }

    <T> void encodesTo(final PojoCodecProvider.Builder builder, final T value, final String json) {
        encodesTo(builder, value, json, false);
    }

    <T> void encodesTo(final PojoCodecProvider.Builder builder, final T value, final String json, final boolean collectible) {
        encodesTo(getCodecRegistry(builder), value, json, collectible);
    }

    <T> void encodesTo(final CodecRegistry registry, final T value, final String json) {
        encodesTo(registry, value, json, false);
    }

    @SuppressWarnings("unchecked")
    <T> void encodesTo(final CodecRegistry registry, final T value, final String json, final boolean collectible) {
        Codec<T> codec = (Codec<T>) registry.get(value.getClass());
        encodesTo(codec, value, json, collectible);
    }

    <T> void encodesTo(final Codec<T> codec, final T value, final String json) {
       encodesTo(codec, value, json, false);
    }

    <T> void encodesTo(final Codec<T> codec, final T value, final String json, final boolean collectible) {
        OutputBuffer encoded = encode(codec, value, collectible);

        BsonDocument asBsonDocument = decode(DOCUMENT_CODEC, encoded);
        assertEquals("Encoded value", BsonDocument.parse(json), asBsonDocument);
    }

    <T> void decodesTo(final PojoCodecProvider.Builder builder, final String json, final T expected) {
        decodesTo(getCodecRegistry(builder), json, expected);
    }

    @SuppressWarnings("unchecked")
    <T> void decodesTo(final CodecRegistry registry, final String json, final T expected) {
        Codec<T> codec = (Codec<T>) registry.get(expected.getClass());
        decodesTo(codec, json, expected);
    }

    <T> void decodesTo(final Codec<T> codec, final String json, final T expected) {
        OutputBuffer encoded = encode(DOCUMENT_CODEC, BsonDocument.parse(json), false);
        T result = decode(codec, encoded);
        assertEquals("Decoded value", expected, result);
    }

    <T> void decodingShouldFail(final Codec<T> codec, final String json) {
        decodesTo(codec, json, null);
    }

    <T> OutputBuffer encode(final Codec<T> codec, final T value, final boolean collectible) {
        OutputBuffer buffer = new BasicOutputBuffer();
        BsonWriter writer = new BsonBinaryWriter(buffer);
        codec.encode(writer, value, EncoderContext.builder().isEncodingCollectibleDocument(collectible).build());
        return buffer;
    }

    <T> T decode(final Codec<T> codec, final OutputBuffer buffer) {
        BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray()))));
        return codec.decode(reader, DecoderContext.builder().build());
    }

    static PojoCodecProvider.Builder getPojoCodecProviderBuilder(final Class<?>... classes) {
        PojoCodecProvider.Builder builder = PojoCodecProvider.builder();
        for (final Class<?> clazz : classes) {
            builder.register(clazz);
        }
        builder.conventions(DEFAULT_CONVENTIONS);
        return builder;
    }

    <T> PojoCodecImpl<T> getCodec(final PojoCodecProvider.Builder builder, final Class<T> clazz) {
        return (PojoCodecImpl<T>) getCodecRegistry(builder).get(clazz);
    }

    <T> PojoCodecImpl<T> getCodec(final Class<T> clazz) {
        return getCodec(getPojoCodecProviderBuilder(clazz), clazz);
    }

    PojoCodecProvider.Builder getPojoCodecProviderBuilder(final ClassModelBuilder<?>... classModelBuilders) {
        List<ClassModel<?>> builders = new ArrayList<>();
        for (ClassModelBuilder<?> classModelBuilder : classModelBuilders) {
            builders.add(classModelBuilder.build());
        }
        return PojoCodecProvider.builder().register(builders.toArray(new ClassModel<?>[builders.size()]));
    }

    CodecRegistry getCodecRegistry(final PojoCodecProvider.Builder builder) {
        return fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider(), new Jsr310CodecProvider(), new EnumCodecProvider(),
                builder.build());
    }

    static SimpleModel getSimpleModel() {
        return new SimpleModel(42, "myString");
    }

    static PrimitivesModel getPrimitivesModel() {
        return new PrimitivesModel(true, Byte.parseByte("1", 2), '1', 1.0, 2f, 3, 5L, (short) 6);
    }

    SimpleGenericsModel<String, String, Integer> getSimpleGenericsModel() {
        HashMap<String, Integer> map = new HashMap<>();
        map.put("D", 2);
        map.put("E", 3);
        map.put("F", 4);

        return new SimpleGenericsModel<>(42, "A", asList("B", "C"), map);
    }

    static SimpleGenericsModel<Long, String, Integer> getSimpleGenericsModelAlt() {
        HashMap<String, Integer> map = new HashMap<>();
        map.put("D", 2);
        map.put("E", 3);
        map.put("F", 4);

        return new SimpleGenericsModel<>(42, 101L, asList("B", "C"), map);
    }

    static ConcreteCollectionsModel getConcreteCollectionsModel() {
        Collection<Integer> collection = asList(1, 2, 3);
        List<Integer> list = asList(4, 5, 6);
        LinkedList<Integer> linked = new LinkedList<>(asList(7, 8, 9));
        Map<String, Double> map = new HashMap<>();
        map.put("A", 1.1);
        map.put("B", 2.2);
        map.put("C", 3.3);
        ConcurrentHashMap<String, Double> concurrent = new ConcurrentHashMap<>();
        concurrent.put("D", 4.4);
        concurrent.put("E", 5.5);
        concurrent.put("F", 6.6);

        return new ConcreteCollectionsModel(collection, list, linked, map, concurrent);
    }


    static ConcreteCollectionsModel getConcreteCollectionsModelWithNulls() {
        Collection<Integer> collection = asList(1, null, 3);
        List<Integer> list = asList(4, null, 6);
        LinkedList<Integer> linked = new LinkedList<>(asList(null, 8, 9));
        Map<String, Double> map = new HashMap<>();
        map.put("A", 1.1);
        map.put("B", null);
        map.put("C", 3.3);

        return new ConcreteCollectionsModel(collection, list, linked, map, null);
    }

    static SimpleNestedPojoModel getSimpleNestedPojoModel() {
        SimpleModel simpleModel = getSimpleModel();
        return new SimpleNestedPojoModel(simpleModel);
    }

    static CollectionNestedPojoModel getCollectionNestedPojoModel() {
        return getCollectionNestedPojoModel(false);
    }

    static CollectionNestedPojoModel getCollectionNestedPojoModelWithNulls() {
        return getCollectionNestedPojoModel(true);
    }

    private static CollectionNestedPojoModel getCollectionNestedPojoModel(final boolean useNulls) {
        List<SimpleModel> listSimple;
        Set<SimpleModel> setSimple;
        Map<String, SimpleModel> mapSimple;

        if (useNulls) {
            listSimple = null;
            setSimple = null;
            mapSimple = null;
        } else {
            SimpleModel simpleModel = getSimpleModel();
            listSimple = singletonList(simpleModel);
            setSimple = new HashSet<>(listSimple);
            mapSimple = new HashMap<>();
            mapSimple.put("s", simpleModel);
        }

        List<List<SimpleModel>> listListSimple = singletonList(listSimple);
        Set<Set<SimpleModel>> setSetSimple = new HashSet<>(singletonList(setSimple));

        Map<String, Map<String, SimpleModel>> mapMapSimple = new HashMap<>();
        mapMapSimple.put("ms", mapSimple);

        Map<String, List<SimpleModel>> mapListSimple = new HashMap<>();
        mapListSimple.put("ls", listSimple);

        Map<String, List<Map<String, SimpleModel>>> mapListMapSimple = new HashMap<>();
        mapListMapSimple.put("lm", singletonList(mapSimple));

        Map<String, Set<SimpleModel>> mapSetSimple = new HashMap<>();
        mapSetSimple.put("s", setSimple);

        List<Map<String, SimpleModel>> listMapSimple = singletonList(mapSimple);
        List<Map<String, List<SimpleModel>>> listMapListSimple = singletonList(mapListSimple);
        List<Map<String, Set<SimpleModel>>> listMapSetSimple = singletonList(mapSetSimple);

        return new CollectionNestedPojoModel(listSimple, listListSimple, setSimple, setSetSimple, mapSimple, mapMapSimple, mapListSimple,
                mapListMapSimple, mapSetSimple, listMapSimple, listMapListSimple, listMapSetSimple);
    }

    static ConventionModel getConventionModel() {
        SimpleModel simpleModel = getSimpleModel();
        ConventionModel child = new ConventionModel("child", null, simpleModel);
        return new ConventionModel("id", child, null);
    }

    static ShapeModelCircle getShapeModelCircle() {
        return new ShapeModelCircle("orange", 4.2);
    }

    static ShapeModelRectangle getShapeModelRectangle() {
        return new ShapeModelRectangle("green", 22.1, 105.0);
    }

    static MultipleBoundsModel getMultipleBoundsModel() {
        HashMap<String, String> map = new HashMap<>();
        map.put("key", "value");
        List<Integer> list = asList(1, 2, 3);
        return new MultipleBoundsModel(map, list, 2.2);
    }

    static NestedGenericHolderFieldWithMultipleTypeParamsModel getNestedGenericHolderFieldWithMultipleTypeParamsModel() {
        SimpleGenericsModel<Long, String, Integer> simple = getSimpleGenericsModelAlt();
        PropertyWithMultipleTypeParamsModel<Integer, Long, String> field =
                new PropertyWithMultipleTypeParamsModel<>(simple);
        GenericHolderModel<PropertyWithMultipleTypeParamsModel<Integer, Long, String>> nested = new
                GenericHolderModel<>(field, 42L);
        return new NestedGenericHolderFieldWithMultipleTypeParamsModel(nested);
    }

    static NestedGenericHolderSimpleGenericsModel getNestedGenericHolderSimpleGenericsModel() {
        SimpleModel simpleModel = getSimpleModel();
        Map<String, SimpleModel> map = new HashMap<>();
        map.put("A", simpleModel);
        Map<String, Map<String, SimpleModel>> mapB = new HashMap<>();
        mapB.put("A", map);
        SimpleGenericsModel<Integer, List<SimpleModel>, Map<String, SimpleModel>> simpleGenericsModel =
                new SimpleGenericsModel<>(42, 42,
                        singletonList(singletonList(simpleModel)), mapB);
        GenericHolderModel<SimpleGenericsModel<Integer, List<SimpleModel>, Map<String, SimpleModel>>> nested =
                new GenericHolderModel<>(simpleGenericsModel, 42L);

        return new NestedGenericHolderSimpleGenericsModel(nested);
    }

    static NestedSelfReferentialGenericHolderModel getNestedSelfReferentialGenericHolderModel() {
        SelfReferentialGenericModel<Boolean, Long> selfRef1 = new SelfReferentialGenericModel<>(true, 33L,
                new SelfReferentialGenericModel<>(44L, false, null));
        SelfReferentialGenericModel<Boolean, Double> selfRef2 = new SelfReferentialGenericModel<>(true, 3.14,
                new SelfReferentialGenericModel<>(3.42, true, null));
        NestedSelfReferentialGenericModel<Boolean, Long, Double> nested =
                new NestedSelfReferentialGenericModel<>(true, 42L, 44.0, selfRef1, selfRef2);
         return new NestedSelfReferentialGenericHolderModel(nested);
    }

    static NestedGenericHolderModel getNestedGenericHolderModel() {
        return new NestedGenericHolderModel(new GenericHolderModel<>("generic", 1L));
    }

    static NestedGenericHolderMapModel getNestedGenericHolderMapModel() {
        Map<String, SimpleModel> mapSimple = new HashMap<>();
        mapSimple.put("s", getSimpleModel());
        return new NestedGenericHolderMapModel(new GenericHolderModel<>(mapSimple, 1L));
    }

    static NestedReusedGenericsModel getNestedReusedGenericsModel() {
        return new NestedReusedGenericsModel(new ReusedGenericsModel<>(1L,
                singletonList(getSimpleModel()), "field3", 42, "field5", asList(getSimpleModel(), getSimpleModel()), 2L, "field8"));
    }

    static GenericTreeModel<String, Integer> getGenericTreeModel() {
        return new GenericTreeModel<>("top", 1,
                new GenericTreeModel<>("left", 2,
                        new GenericTreeModel<>("left", 3, null, null), null),
                new GenericTreeModel<>("right", 4,
                        new GenericTreeModel<>("left", 5, null, null), null));
    }

    static GenericTreeModel<String, String> getGenericTreeModelStrings() {
        return new GenericTreeModel<>("top", "1",
                new GenericTreeModel<>("left", "2",
                        new GenericTreeModel<>("left", "3", null, null), null),
                new GenericTreeModel<>("right", "4",
                        new GenericTreeModel<>("left", "5", null, null), null));
    }

    static InvalidMapModel getInvalidMapModel() {
        Map<Integer, Integer> map = new HashMap<>();
        map.put(1, 1);
        map.put(2, 2);
        return new InvalidMapModel(map);
    }

    static final String SIMPLE_MODEL_JSON = "{'integerField': 42, 'stringField': 'myString'}";

    class StringToObjectIdCodec implements Codec<String> {

        @Override
        public void encode(final BsonWriter writer, final String value, final EncoderContext encoderContext) {
            writer.writeObjectId(new ObjectId(value));
        }

        @Override
        public Class<String> getEncoderClass() {
            return String.class;
        }

        @Override
        public String decode(final BsonReader reader, final DecoderContext decoderContext) {
            return reader.readObjectId().toHexString();
        }
    }

    class SimpleEnumCodec implements Codec<SimpleEnum> {

        @Override
        public void encode(final BsonWriter writer, final SimpleEnum value, final EncoderContext encoderContext) {
            writer.writeInt32(value.ordinal());
        }

        @Override
        public Class<SimpleEnum> getEncoderClass() {
            return SimpleEnum.class;
        }

        @Override
        public SimpleEnum decode(final BsonReader reader, final DecoderContext decoderContext) {
            int ordinal = reader.readInt32();
            switch (ordinal){
                case 0:
                    return SimpleEnum.ALPHA;
                case 1:
                    return SimpleEnum.BRAVO;
                default:
                    return SimpleEnum.CHARLIE;
            }
        }
    }
}
