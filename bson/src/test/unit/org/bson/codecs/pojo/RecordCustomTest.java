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

import org.bson.Document;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.LongCodec;
import org.bson.codecs.MapCodec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.entities.InvalidMapPropertyCodecProvider;
import org.bson.codecs.pojo.entities.Optional;
import org.bson.codecs.pojo.entities.OptionalPropertyCodecProvider;
import org.bson.codecs.pojo.entities.SimpleEnum;
import org.bson.codecs.pojo.entities.records.BsonRepresentationRecord;
import org.bson.codecs.pojo.entities.records.BsonRepresentationUnsupportedIntRecord;
import org.bson.codecs.pojo.entities.records.BsonRepresentationUnsupportedStringRecord;
import org.bson.codecs.pojo.entities.records.ConverterRecord;
import org.bson.codecs.pojo.entities.records.CustomPropertyCodecOptionalRecord;
import org.bson.codecs.pojo.entities.records.GenericHolderRecord;
import org.bson.codecs.pojo.entities.records.GenericTreeRecord;
import org.bson.codecs.pojo.entities.records.InvalidCollectionRecord;
import org.bson.codecs.pojo.entities.records.InvalidMapRecord;
import org.bson.codecs.pojo.entities.records.MapStringObjectRecord;
import org.bson.codecs.pojo.entities.records.PrimitivesRecord;
import org.bson.codecs.pojo.entities.records.SimpleEnumRecord;
import org.bson.codecs.pojo.entities.records.SimpleNestedRecord;
import org.bson.codecs.pojo.entities.records.SimpleRecord;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.Assert.assertTrue;

public class RecordCustomTest extends PojoTestCase {

    @Test
    public void testEnumSupportWithCustomCodec() {
        CodecRegistry registry = fromRegistries(getRecordCodecRegistry(SimpleEnumRecord.class), fromCodecs(new SimpleEnumCodec()));
        roundTrip(registry, new SimpleEnumRecord(SimpleEnum.BRAVO), "{ 'myEnum': 1 }");
    }

    @Test
    public void testCustomCodec() {
        ObjectId id = new ObjectId();
        ConverterRecord record = new ConverterRecord(id.toHexString(), "myName");

        ClassModelBuilder<ConverterRecord> classModel = ClassModel.builder(ConverterRecord.class);
        PropertyModelBuilder<String> idPropertyModelBuilder = (PropertyModelBuilder<String>) classModel.getProperty("id");
        idPropertyModelBuilder.codec(new StringToObjectIdCodec());

        roundTrip(getRecordCodecRegistry(classModel), record,
                format("{'_id': {'$oid': '%s'}, 'name': 'myName'}", id.toHexString()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCustomPropertySerializer() {
        SimpleRecord record = new SimpleRecord(null, "myString");
        ClassModelBuilder<SimpleRecord> classModel = ClassModel.builder(SimpleRecord.class);
        ((PropertyModelBuilder<Integer>) classModel.getProperty("integerField"))
                .propertySerialization(new PropertySerialization<Integer>() {
                    @Override
                    public boolean shouldSerialize(final Integer value) {
                        return true;
                    }
                });

        roundTrip(getRecordCodecRegistry(classModel), record, "{'integerField': null, 'stringField': 'myString'}");
    }

    @Test
    public void testCanHandleExtraData() {
        decodesTo(getRecordCodecRegistry(SimpleRecord.class), "{'integerField': 42,  'stringField': 'myString', 'extraFieldA': 1, 'extraFieldB': 2}",
                RecordRoundTripTest.getSimpleRecord());
    }

    @Test
    public void testDataCanHandleMissingData() {
        SimpleRecord record = new SimpleRecord(null, "myString");

        decodesTo(getRecordCodecRegistry(SimpleRecord.class), "{'_t': 'SimpleRecord', 'stringField': 'myString'}", record);
    }

    @Test
    public void testCanHandleTopLevelGenericIfHasCodec() {
        GenericHolderRecord<Long> record = new GenericHolderRecord<>(1L, 2L);

        ClassModelBuilder<GenericHolderRecord> classModelBuilder = ClassModel.builder(GenericHolderRecord.class);
        ((PropertyModelBuilder<Long>) classModelBuilder.getProperty("myGenericField")).codec(new LongCodec());
        RecordCodecProvider provider = RecordCodecProvider.builder().register(classModelBuilder.build()).build();

        roundTrip(RecordRoundTripTest.getRegistryFromProvider(provider), record,
                "{'myGenericField': {'$numberLong': '1'}, 'myLongField': {'$numberLong': '2'}}");
    }

    @Test
    public void testCustomRegisteredPropertyCodecWithValue() {
        CustomPropertyCodecOptionalRecord record = new CustomPropertyCodecOptionalRecord(Optional.of("foo"));
        ClassModelBuilder<CustomPropertyCodecOptionalRecord> classModelBuilder = ClassModel.builder(CustomPropertyCodecOptionalRecord.class);
        RecordCodecProvider.Builder provider = RecordCodecProvider.builder();
        provider.register(classModelBuilder.build());
        provider.register(new OptionalPropertyCodecProvider());

        roundTrip(RecordRoundTripTest.getRegistryFromProvider(provider.build()), record,
                "{'optionalField': 'foo'}");
    }

    @Test
    public void testMapStringObjectRecord() {
        MapStringObjectRecord record = new MapStringObjectRecord(new HashMap<String, Object>(Document.parse("{a : 1, b: 'b', c: [1, 2, 3]}")));
        CodecRegistry registry = fromRegistries(fromCodecs(new MapCodec()),
                getRecordCodecRegistry(MapStringObjectRecord.class));
        roundTrip(registry, record, "{ map: {a : 1, b: 'b', c: [1, 2, 3]}}");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMapStringObjectRecordWithObjectCodec() {
        MapStringObjectRecord record = new MapStringObjectRecord(new HashMap<String, Object>(Document.parse("{a : 1, b: 'b', c: [1, 2, 3]}")));
        CodecRegistry registry = fromRegistries(fromCodecs(new MapCodec()), fromCodecs(new PojoCustomTest.ObjectCodec()), getRecordCodecRegistry(MapStringObjectRecord.class));
        roundTrip(registry, record, "{ map: {a : 1, b: 'b', c: [1, 2, 3]}}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testEncodingInvalidMapRecord() {
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        map.put(1, 1);
        map.put(2, 2);
       InvalidMapRecord record = new InvalidMapRecord(map);

        encodesTo(getRecordCodecRegistry(InvalidMapRecord.class), record, "{'invalidMap': {'1': 1, '2': 2}}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testDecodingInvalidMapRecord() {
        try {
            decodingShouldFail(getRecordCodec(InvalidMapRecord.class), "{'invalidMap': {'1': 1, '2': 2}}");
        } catch (CodecConfigurationException e) {
            e.printStackTrace();
            assertTrue(e.getMessage().startsWith("Failed to decode 'InvalidMapRecord'. Decoding 'invalidMap' errored with:"));
            throw e;
        }
    }

    @Test(expected = CodecConfigurationException.class)
    public void testEncodingInvalidCollectionRecord() {
        try {
            encodesTo(getRecordCodecRegistry(InvalidCollectionRecord.class), new InvalidCollectionRecord(asList(1, 2, 3)),
                    "{collectionField: [1, 2, 3]}");
        } catch (CodecConfigurationException e) {
            assertTrue(e.getMessage().startsWith("Failed to encode 'InvalidCollectionRecord'. Encoding 'collectionField' errored with:"));
            throw e;
        }
    }

    @Test
    public void testInvalidMapRecordWithCustomPropertyCodecProvider() {
        Map<Integer, Integer> map = new HashMap<>();
        map.put(1, 1);
        map.put(2, 2);
        InvalidMapRecord record = new InvalidMapRecord(map);
        ClassModelBuilder builder = ClassModel.builder(InvalidMapRecord.class);
        RecordCodecProvider.Builder provider = RecordCodecProvider.builder();
        provider.register(InvalidMapRecord.class);
        provider.register(new InvalidMapPropertyCodecProvider());
        encodesTo(RecordRoundTripTest.getRegistryFromProvider(provider.build()), record,
                "{'invalidMap': {'1': 1, '2': 2}}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testDataUnknownClass() {
        ClassModel<SimpleRecord> classModel = ClassModel.builder(SimpleRecord.class).enableDiscriminator(true).build();
        try {
            RecordCodecProvider provider = RecordCodecProvider.builder().register(classModel).build();
            decodingShouldFail(RecordRoundTripTest.getRegistryFromProvider(provider).get(SimpleRecord.class), "{'_t': 'FakeRecord'}");
        } catch (CodecConfigurationException e) {
            assertTrue(e.getMessage().startsWith("Failed to decode 'SimpleRecord'. Decoding errored with:"));
            throw e;
        }
    }

    @Test(expected = CodecConfigurationException.class)
    public void testInvalidTypeForField() {
        decodingShouldFail(getRecordCodec(SimpleRecord.class), "{'_t': 'SimpleRecord', 'stringField': 123}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testInvalidTypeForPrimitiveField() {
        decodingShouldFail(getRecordCodec(PrimitivesRecord.class), "{ '_t': 'PrimitivesRecord', 'myBoolean': null}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testInvalidTypeForRecordField() {
        decodingShouldFail(getRecordCodec(SimpleNestedRecord.class), "{ '_t': 'SimpleNestedRecord', 'simple': 123}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testInvalidDiscriminatorInNestedRecord() {
        decodingShouldFail(getRecordCodec(SimpleNestedRecord.class), "{ '_t': 'SimpleNestedRecord',"
                + "'simple': {'_t': 'FakeRecord', 'integerField': 42, 'stringField': 'myString'}}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testCannotEncodeUnspecializedClasses() {
        ClassModelBuilder<GenericTreeRecord> builder = ClassModel.builder(GenericTreeRecord.class);
        CodecRegistry registry = fromProviders(getRecordProviderFromClassModel(builder));
        encode(registry.get(GenericTreeRecord.class), RecordRoundTripTest.getGenericTreeRecord(), false);
    }

    @Test(expected = CodecConfigurationException.class)
    public void testCannotDecodeUnspecializedClasses() {
        decodingShouldFail(getRecordCodec(GenericTreeRecord.class),
                "{'field1': 'top', 'field2': 1, "
                        + "'left': {'field1': 'left', 'field2': 2, 'left': {'field1': 'left', 'field2': 3}}, "
                        + "'right': {'field1': 'right', 'field2': 4, 'left': {'field1': 'left', 'field2': 5}}}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testInvalidBsonRepresentationStringDecoding() {
        decodingShouldFail(getRecordCodec(BsonRepresentationUnsupportedStringRecord.class), "{'id': 'hello', s: 3}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testInvalidBsonRepresentationStringEncoding() {
        encodesTo(getRecordCodecRegistry(BsonRepresentationUnsupportedStringRecord.class),
                new BsonRepresentationUnsupportedStringRecord("1", "1"), "");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testInvalidBsonRepresentationIntDecoding() {
        decodingShouldFail(getRecordCodec(BsonRepresentationUnsupportedIntRecord.class), "{'id': 'hello', s: '3'}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testStringIdIsNotObjectId() {
        encodesTo(getRecordCodecRegistry(BsonRepresentationUnsupportedIntRecord.class), new BsonRepresentationRecord("notanobjectid", 1), null);
    }

    private static <T> RecordCodecProvider getRecordProviderFromClassModel(ClassModelBuilder<T> classModelBuilder) {
        return RecordCodecProvider.builder().register(classModelBuilder.build()).build();
    }

    private static <T> CodecRegistry getRecordCodecRegistry(ClassModelBuilder<T> classModelBuilder) {
        return fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider(), getRecordProviderFromClassModel(classModelBuilder));
    }

    private static CodecRegistry getRecordCodecRegistry(Class<?> clazz) {
        ClassModelBuilder<?> builder = ClassModel.builder(clazz);
        return getRecordCodecRegistry(builder);
    }

    private static Codec<?> getRecordCodec(Class<?> clazz) {
        return getRecordCodecRegistry(clazz).get(clazz);
    }

}
