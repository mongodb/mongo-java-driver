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

import org.bson.BsonDocument;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.entities.SimpleEnum;
import org.bson.codecs.pojo.entities.records.AnnotationBsonPropertyIdRecord;
import org.bson.codecs.pojo.entities.records.BsonIgnoreInvalidMapRecord;
import org.bson.codecs.pojo.entities.records.BsonPropUnderscoreId;
import org.bson.codecs.pojo.entities.records.BsonPropUnderscoreIdNonIdName;
import org.bson.codecs.pojo.entities.records.BsonRepresentationRecord;
import org.bson.codecs.pojo.entities.records.CollectionNestedRecord;
import org.bson.codecs.pojo.entities.records.ConcreteCollectionsRecord;
import org.bson.codecs.pojo.entities.records.ConcreteInterfaceGenericRecord;
import org.bson.codecs.pojo.entities.records.ContainsAlternativeMapAndCollectionRecord;
import org.bson.codecs.pojo.entities.records.ConventionRecord;
import org.bson.codecs.pojo.entities.records.GenericHolderRecord;
import org.bson.codecs.pojo.entities.records.GenericTreeRecord;
import org.bson.codecs.pojo.entities.records.MultipleLevelGenericRecord;
import org.bson.codecs.pojo.entities.records.NestedFieldReusingClassTypeParameterRecord;
import org.bson.codecs.pojo.entities.records.NestedGenericHolderFieldWithMultipleTypeParamsRecord;
import org.bson.codecs.pojo.entities.records.NestedGenericHolderMapRecord;
import org.bson.codecs.pojo.entities.records.NestedGenericHolderRecord;
import org.bson.codecs.pojo.entities.records.NestedGenericHolderSimpleGenericsRecord;
import org.bson.codecs.pojo.entities.records.NestedGenericTreeRecord;
import org.bson.codecs.pojo.entities.records.NestedMultipleLevelGenericRecord;
import org.bson.codecs.pojo.entities.records.NestedReusedGenericsRecord;
import org.bson.codecs.pojo.entities.records.NestedSelfReferentialGenericHolderRecord;
import org.bson.codecs.pojo.entities.records.NestedSelfReferentialGenericRecord;
import org.bson.codecs.pojo.entities.records.NestedSimpleIdRecord;
import org.bson.codecs.pojo.entities.records.PrimitivesRecord;
import org.bson.codecs.pojo.entities.records.PropertyReusingClassTypeParameterRecord;
import org.bson.codecs.pojo.entities.records.PropertySelectionRecord;
import org.bson.codecs.pojo.entities.records.PropertyWithMultipleTypeParamsRecord;
import org.bson.codecs.pojo.entities.records.ReusedGenericsRecord;
import org.bson.codecs.pojo.entities.records.SelfReferentialGenericRecord;
import org.bson.codecs.pojo.entities.records.SimpleEnumRecord;
import org.bson.codecs.pojo.entities.records.SimpleGenericsRecord;
import org.bson.codecs.pojo.entities.records.SimpleIdRecord;
import org.bson.codecs.pojo.entities.records.SimpleNestedRecord;
import org.bson.codecs.pojo.entities.records.SimpleRecord;
import org.bson.codecs.pojo.entities.records.TreeWithIdRecord;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

@RunWith(Parameterized.class)
public class RecordRoundTripTest extends PojoTestCase {
    private final String name;
    private final Record record;
    private final CodecRegistry registry;
    private final String json;

    public RecordRoundTripTest(final String name, final Record record, final CodecRegistry registry, final String json) {
        this.name = name;
        this.record = record;
        this.registry = registry;
        this.json = json;
    }

    @Test
    public void test() { roundTrip(registry, record, json); }

    private static List<TestData> testCases() {
        List<TestData> data = new ArrayList<>();
        data.add(new TestData("Simple record", getSimpleRecord(),
                getRecordRegistryFromClasses(SimpleRecord.class), SIMPLE_RECORD_JSON));

        data.add(new TestData("Property selection record", new PropertySelectionRecord("stringField"),
                getRecordRegistryFromClasses(PropertySelectionRecord.class), "{'stringField': 'stringField'}"));

        data.add(new TestData("Conventions default", getConventionRecord(),
                getRecordRegistryFromClasses(ConventionRecord.class, SimpleRecord.class),
                "{'_id': 'id', '_cls': 'AnnotatedConventionRecord', "
                        + "'child': {'_id': 'child',"
                        + "'record': {'integerField': 42, 'stringField': 'myString'}}}"));

        data.add(new TestData("BsonIgnore invalid map", new BsonIgnoreInvalidMapRecord("stringField", null),
                getRecordRegistryFromClasses(BsonIgnoreInvalidMapRecord.class), "{'stringField': 'stringField'}"));

        data.add(new TestData("Concrete generic interface record", new ConcreteInterfaceGenericRecord("someValue"),
                getRecordRegistryFromClasses(ConcreteInterfaceGenericRecord.class), "{propertyA: 'someValue'}"));

        data.add(new TestData("Primitives Record", getPrimitivesRecord(),
                getRecordRegistryFromClasses(PrimitivesRecord.class),
                "{ 'myBoolean': true, 'myByte': 1, 'myCharacter': '1', 'myDouble': 1.0, 'myFloat': 2.0," +
                        " 'myInteger': 3, 'myLong': { '$numberLong': '5' }, 'myShort': 6}"));

        data.add(new TestData("Concrete collections record", getConcreteCollectionsRecord(),
                getRecordRegistryFromClasses(ConcreteCollectionsRecord.class),
                "{'collection': [1, 2, 3], 'list': [4, 5, 6], 'linked': [7, 8, 9], 'map': {'A': 1.1, 'B': 2.2, 'C': 3.3},"
                        + "'concurrent': {'D': 4.4, 'E': 5.5, 'F': 6.6}}"));

        data.add(new TestData("Handling of nulls inside collections", getConcreteCollectionsRecordWithNulls(),
                getRecordRegistryFromClasses(ConcreteCollectionsRecord.class),
                "{'collection': [1, null, 3], 'list': [4, null, 6], 'linked': [null, 8, 9], 'map': {'A': 1.1, 'B': null, 'C': 3.3}}"));

        data.add(new TestData("Nested simple", getSimpleNestedRecord(),
                getRecordRegistryFromClasses(SimpleNestedRecord.class, SimpleRecord.class),
                "{'simple': " + SIMPLE_RECORD_JSON + "}"));

        data.add(new TestData("Nested collection", getCollectionNestedRecord(),
                getRecordRegistryFromClasses(CollectionNestedRecord.class, SimpleRecord.class),
                "{ 'listSimple': [" + SIMPLE_RECORD_JSON + "],"
                        + "'listListSimple': [[" + SIMPLE_RECORD_JSON + "]],"
                        + "'setSimple': [" + SIMPLE_RECORD_JSON + "],"
                        + "'setSetSimple': [[" + SIMPLE_RECORD_JSON + "]],"
                        + "'mapSimple': {'s': " + SIMPLE_RECORD_JSON + "},"
                        + "'mapMapSimple': {'ms': {'s': " + SIMPLE_RECORD_JSON + "}},"
                        + "'mapListSimple': {'ls': [" + SIMPLE_RECORD_JSON + "]},"
                        + "'mapListMapSimple': {'lm': [{'s': " + SIMPLE_RECORD_JSON + "}]},"
                        + "'mapSetSimple': {'s': [" + SIMPLE_RECORD_JSON + "]},"
                        + "'listMapSimple': [{'s': " + SIMPLE_RECORD_JSON + "}],"
                        + "'listMapListSimple': [{'ls': [" + SIMPLE_RECORD_JSON + "]}],"
                        + "'listMapSetSimple': [{'s': [" + SIMPLE_RECORD_JSON + "]}],"
                        + "}"));

        data.add(new TestData("Nested collection with nulls", getCollectionNestedRecordWithNulls(),
                getRecordRegistryFromClasses(CollectionNestedRecord.class, SimpleRecord.class),
                "{ 'listListSimple': [ null ],"
                        + "'setSetSimple': [ null ],"
                        + "'mapMapSimple': {'ms': null},"
                        + "'mapListSimple': {'ls': null},"
                        + "'mapListMapSimple': {'lm': [null]},"
                        + "'mapSetSimple': {'s': null},"
                        + "'listMapSimple': [null],"
                        + "'listMapListSimple': [{'ls': null}],"
                        + "'listMapSetSimple': [{'s': null}],"
                        + "}"));

        data.add(new TestData("Nested generic holder", getNestedGenericHolderRecord(),
                getRecordRegistryFromClasses(NestedGenericHolderRecord.class, GenericHolderRecord.class),
                "{'nested': {'myGenericField': 'generic', 'myLongField': {'$numberLong': '1'}}}"));

        data.add(new TestData("Nested generic holder map", getNestedGenericHolderMapRecord(),
                getRecordRegistryFromClasses(NestedGenericHolderMapRecord.class, GenericHolderRecord.class, SimpleRecord.class),
                "{ 'nested': { 'myGenericField': {'s': " + SIMPLE_RECORD_JSON + "}, 'myLongField': {'$numberLong': '1'}}}"));

        data.add(new TestData("Nested reused generic", getNestedReusedGenericsRecord(),
                getRecordRegistryFromClasses(NestedReusedGenericsRecord.class, SimpleRecord.class, NestedReusedGenericsRecord.class,
                        ReusedGenericsRecord.class, GenericHolderRecord.class),
                "{ 'nested':{ 'field1':{ '$numberLong':'1' }, 'field2':[" + SIMPLE_RECORD_JSON + "], "
                        + "'field3':'field3', 'field4':42, 'field5':'field5', 'field6':[" + SIMPLE_RECORD_JSON + ", "
                        + SIMPLE_RECORD_JSON + "], 'field7':{ '$numberLong':'2' }, 'field8':'field8' } }"));

        data.add(new TestData("Nested generic holder with multiple types", getNestedGenericHolderFieldWithMultipleTypeParamsRecord(),
                getRecordRegistryFromClasses(NestedGenericHolderFieldWithMultipleTypeParamsRecord.class, SimpleGenericsRecord.class,
                        GenericHolderRecord.class, PropertyWithMultipleTypeParamsRecord.class),
                "{'nested': {'myGenericField': {_t: 'PropertyWithMultipleTypeParamsRecord', "
                        + "'simpleGenericsRecord': {_t: 'org.bson.codecs.pojo.entities.records.SimpleGenericsRecord', 'myIntegerField': 42, "
                        + "'myGenericField': {'$numberLong': '101'}, 'myListField': ['B', 'C'], 'myMapField': {'D': 2, 'E': 3, 'F': 4 }}},"
                        + "'myLongField': {'$numberLong': '42'}}}"));

        data.add(new TestData("Nested generic tree", new NestedGenericTreeRecord(42, getGenericTreeRecord()),
                getRecordRegistryFromClasses(NestedGenericTreeRecord.class, GenericTreeRecord.class),
                "{'intField': 42, 'nested': {'field1': 'top', 'field2': 1, "
                        + "'left': {'field1': 'left', 'field2': 2, 'left': {'field1': 'left', 'field2': 3}}, "
                        + "'right': {'field1': 'right', 'field2': 4, 'left': {'field1': 'left', 'field2': 5}}}}"));

        data.add(new TestData("Nested multiple level",
                new NestedMultipleLevelGenericRecord(42, new MultipleLevelGenericRecord<String>("string", getGenericTreeRecord())),
                getRecordRegistryFromClasses(NestedMultipleLevelGenericRecord.class, MultipleLevelGenericRecord.class, GenericTreeRecord.class),
                "{'intField': 42, 'nested': {'stringField': 'string', 'nested': {'field1': 'top', 'field2': 1, "
                        + "'left': {'field1': 'left', 'field2': 2, 'left': {'field1': 'left', 'field2': 3}}, "
                        + "'right': {'field1': 'right', 'field2': 4, 'left': {'field1': 'left', 'field2': 5}}}}}"));

        data.add(new TestData("Nested Generics holder", getNestedGenericHolderSimpleGenericsRecord(),
                getRecordRegistryFromClasses(NestedGenericHolderSimpleGenericsRecord.class, GenericHolderRecord.class, SimpleGenericsRecord.class, SimpleRecord.class),
                "{'nested': {'myGenericField': {'myIntegerField': 42, 'myGenericField': 42,"
                        + "                           'myListField': [[" + SIMPLE_RECORD_JSON + "]], "
                        + "                           'myMapField': {'A': {'A': " + SIMPLE_RECORD_JSON + "}}},"
                        + "         'myLongField': {'$numberLong': '42' }}}"));

        data.add(new TestData("Nested property reusing type parameter",
                new NestedFieldReusingClassTypeParameterRecord(new PropertyReusingClassTypeParameterRecord<String>(getGenericTreeRecordStrings())),
                getRecordRegistryFromClasses(NestedFieldReusingClassTypeParameterRecord.class, PropertyReusingClassTypeParameterRecord.class,
                        GenericTreeRecord.class),
                "{'nested': {'tree': {'field1': 'top', 'field2': '1', "
                        + "'left': {'field1': 'left', 'field2': '2', 'left': {'field1': 'left', 'field2': '3'}}, "
                        + "'right': {'field1': 'right', 'field2': '4', 'left': {'field1': 'left', 'field2': '5'}}}}}"));

        data.add(new TestData("Self referential", getNestedSelfReferentialGenericHolderRecord(),
                getRecordRegistryFromClasses(NestedSelfReferentialGenericHolderRecord.class, NestedSelfReferentialGenericRecord.class,
                        SelfReferentialGenericRecord.class),
                "{'nested': { 't': true, 'v': {'$numberLong': '42'}, 'z': 44.0, "
                        + "'selfRef1': {'t': true, 'v': {'$numberLong': '33'}, 'child': {'t': {'$numberLong': '44'}, 'v': false}}, "
                        + "'selfRef2': {'t': true, 'v': 3.14, 'child': {'t': 3.42, 'v': true}}}}"));

        BsonDocument document = BsonDocument.parse("{customList: [1,2,3], customMap: {'field': 'value'}}");

        data.add(new TestData("Can handle custom Maps and Collections",
                new ContainsAlternativeMapAndCollectionRecord(document.getArray("customList"), document.getDocument("customMap")),
                getRecordRegistryFromClasses(ContainsAlternativeMapAndCollectionRecord.class),
                "{customList: [1,2,3], customMap: {'field': 'value'}}"));

        data.add(new TestData("Enums support",
                new SimpleEnumRecord(SimpleEnum.BRAVO),
                getRecordRegistryFromClasses(SimpleEnumRecord.class),
                "{ 'myEnum': 'BRAVO' }"));

        data.add(new TestData("AnnotationBsonPropertyIdRecord", new AnnotationBsonPropertyIdRecord(99L),
                getRecordRegistryFromClasses(AnnotationBsonPropertyIdRecord.class),
                "{'id': {'$numberLong': '99' }}"));

        data.add(new TestData("SimpleIdRecord with existing id",
                new SimpleIdRecord(new ObjectId("123412341234123412341234"), 42, "myString"),
                getRecordRegistryFromClasses(SimpleIdRecord.class),
                "{'_id': {'$oid': '123412341234123412341234'}, 'integerField': 42, 'stringField': 'myString'}"));

        data.add(new TestData("NestedSimpleIdRecord",
                new NestedSimpleIdRecord(null, new SimpleIdRecord(null,42, "myString")),
                getRecordRegistryFromClasses(NestedSimpleIdRecord.class, SimpleIdRecord.class),
                "{'nestedSimpleIdRecord': {'integerField': 42, 'stringField': 'myString'}}"));

        data.add(new TestData("TreeWithIdRecord",
                new TreeWithIdRecord(new ObjectId("123412341234123412341234"), "top",
                        new TreeWithIdRecord(null, "left-1", new TreeWithIdRecord(null,"left-2", null, null), null), new TreeWithIdRecord(null,"right-1", null, null)),
                getRecordRegistryFromClasses(TreeWithIdRecord.class),
                "{'_id': {'$oid': '123412341234123412341234'}, 'level': 'top',"
                        + "'left': {'level': 'left-1', 'left': {'level': 'left-2'}},"
                        + "'right': {'level': 'right-1'}}"));

        data.add(new TestData("BsonRepresentation is encoded and decoded correctly", new BsonRepresentationRecord(new ObjectId("111111111111111111111111").toHexString(),1),
                getRecordRegistryFromClasses(BsonRepresentationRecord.class),
                "{'_id': {'$oid': '111111111111111111111111'}, 'age': 1}"));

        data.add(new TestData("BsonProperty is _id", new BsonPropUnderscoreId("123123"), getRecordRegistryFromClasses(BsonPropUnderscoreId.class),
                "{'_id': '123123'}"));

        data.add(new TestData("BsonProperty is _id and the field is not id or _id", new BsonPropUnderscoreIdNonIdName("123123"), getRecordRegistryFromClasses(BsonPropUnderscoreIdNonIdName.class),
                "{'_id': '123123'}"));

        return data;
    }

    private static final String SIMPLE_RECORD_JSON = "{'integerField': 42, 'stringField': 'myString'}";

    static CodecRegistry getRegistryFromProvider(RecordCodecProvider provider) {
        return fromProviders(new BsonValueCodecProvider(), new ValueCodecProvider(), provider);
    }

    static SimpleRecord getSimpleRecord() {
        return new SimpleRecord(42, "myString");
    }

    static GenericTreeRecord<String, Integer> getGenericTreeRecord() {
        return new GenericTreeRecord<String, Integer>("top", 1,
                new GenericTreeRecord<String, Integer>("left", 2,
                        new GenericTreeRecord<String, Integer>("left", 3, null, null), null),
                new GenericTreeRecord<String, Integer>("right", 4,
                        new GenericTreeRecord<String, Integer>("left", 5, null, null), null));
    }

    private static CodecRegistry getRecordRegistryFromClasses(Class<?>... classes) {
        RecordCodecProvider.Builder builder = RecordCodecProvider.builder();
        for (Class<?> clazz : classes) {
            builder.register(clazz);
        }
        RecordCodecProvider recordCodecProvider = builder.build();
        return getRegistryFromProvider(recordCodecProvider);
    }

    private static ConventionRecord getConventionRecord() {
        SimpleRecord simpleRecord = getSimpleRecord();
        ConventionRecord child = new ConventionRecord("child", null, simpleRecord);
        return new ConventionRecord("id", child, null);
    }

    private static PrimitivesRecord getPrimitivesRecord() {
        return new PrimitivesRecord(true, Byte.parseByte("1", 2), '1', 1.0, 2f, 3, 5L, (short) 6);
    }

    private static ConcreteCollectionsRecord getConcreteCollectionsRecord() {
        Collection<Integer> collection = asList(1, 2, 3);
        List<Integer> list = asList(4, 5, 6);
        LinkedList<Integer> linked = new LinkedList<Integer>(asList(7, 8, 9));
        Map<String, Double> map = new HashMap<String, Double>();
        map.put("A", 1.1);
        map.put("B", 2.2);
        map.put("C", 3.3);
        ConcurrentHashMap<String, Double> concurrent = new ConcurrentHashMap<String, Double>();
        concurrent.put("D", 4.4);
        concurrent.put("E", 5.5);
        concurrent.put("F", 6.6);

        return new ConcreteCollectionsRecord(collection, list, linked, map, concurrent);
    }

    private static ConcreteCollectionsRecord getConcreteCollectionsRecordWithNulls() {
        Collection<Integer> collection = asList(1, null, 3);
        List<Integer> list = asList(4, null, 6);
        LinkedList<Integer> linked = new LinkedList<Integer>(asList(null, 8, 9));
        Map<String, Double> map = new HashMap<String, Double>();
        map.put("A", 1.1);
        map.put("B", null);
        map.put("C", 3.3);

        return new ConcreteCollectionsRecord(collection, list, linked, map, null);
    }

    private static SimpleNestedRecord getSimpleNestedRecord() {
        SimpleRecord simpleRecord = getSimpleRecord();
        return new SimpleNestedRecord(simpleRecord);
    }

    private static CollectionNestedRecord getCollectionNestedRecord() {
        return getCollectionNestedRecord(false);
    }
    private static CollectionNestedRecord getCollectionNestedRecordWithNulls() {
        return getCollectionNestedRecord(true);
    }

    private static CollectionNestedRecord getCollectionNestedRecord (final boolean useNulls) {
        List<SimpleRecord> listSimple;
        Set<SimpleRecord> setSimple;
        Map<String, SimpleRecord> mapSimple;

        if (useNulls) {
            listSimple = null;
            setSimple = null;
            mapSimple = null;
        } else {
            SimpleRecord SimpleRecord = getSimpleRecord();
            listSimple = singletonList(SimpleRecord);
            setSimple = new HashSet<SimpleRecord>(listSimple);
            mapSimple = new HashMap<String, SimpleRecord>();
            mapSimple.put("s", SimpleRecord);
        }

        List<List<SimpleRecord>> listListSimple = singletonList(listSimple);
        Set<Set<SimpleRecord>> setSetSimple = new HashSet<Set<SimpleRecord>>(singletonList(setSimple));

        Map<String, Map<String, SimpleRecord>> mapMapSimple = new HashMap<String, Map<String, SimpleRecord>>();
        mapMapSimple.put("ms", mapSimple);

        Map<String, List<SimpleRecord>> mapListSimple = new HashMap<String, List<SimpleRecord>>();
        mapListSimple.put("ls", listSimple);

        Map<String, List<Map<String, SimpleRecord>>> mapListMapSimple = new HashMap<String, List<Map<String, SimpleRecord>>>();
        mapListMapSimple.put("lm", singletonList(mapSimple));

        Map<String, Set<SimpleRecord>> mapSetSimple = new HashMap<String, Set<SimpleRecord>>();
        mapSetSimple.put("s", setSimple);

        List<Map<String, SimpleRecord>> listMapSimple = singletonList(mapSimple);
        List<Map<String, List<SimpleRecord>>> listMapListSimple = singletonList(mapListSimple);
        List<Map<String, Set<SimpleRecord>>> listMapSetSimple = singletonList(mapSetSimple);

        return new CollectionNestedRecord(listSimple, listListSimple, setSimple, setSetSimple, mapSimple, mapMapSimple, mapListSimple,
                mapListMapSimple, mapSetSimple, listMapSimple, listMapListSimple, listMapSetSimple);
    }

    private static NestedGenericHolderRecord getNestedGenericHolderRecord() {
        return new NestedGenericHolderRecord(new GenericHolderRecord<String>("generic", 1L));
    }

    private static NestedGenericHolderMapRecord getNestedGenericHolderMapRecord() {
        Map<String, SimpleRecord> mapSimple = new HashMap<>();
        mapSimple.put("s", getSimpleRecord());
        return new NestedGenericHolderMapRecord(new GenericHolderRecord<Map<String, SimpleRecord>>(mapSimple, 1L));
    }

    private static NestedReusedGenericsRecord getNestedReusedGenericsRecord() {
        return new NestedReusedGenericsRecord(new ReusedGenericsRecord<Long, List<SimpleRecord>, String>(1L,
                singletonList(getSimpleRecord()), "field3", 42, "field5", asList(getSimpleRecord(), getSimpleRecord()), 2L, "field8"));
    }

    private static SimpleGenericsRecord<Long, String, Integer> getSimpleGenericsRecordAlt() {
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        map.put("D", 2);
        map.put("E", 3);
        map.put("F", 4);

        return new SimpleGenericsRecord<Long, String, Integer>(42, 101L, asList("B", "C"), map);
    }

    private static NestedGenericHolderFieldWithMultipleTypeParamsRecord getNestedGenericHolderFieldWithMultipleTypeParamsRecord() {
        SimpleGenericsRecord<Long, String, Integer> simple = getSimpleGenericsRecordAlt();
        PropertyWithMultipleTypeParamsRecord<Integer, Long, String> field =
                new PropertyWithMultipleTypeParamsRecord<>(simple);
        GenericHolderRecord<PropertyWithMultipleTypeParamsRecord<Integer, Long, String>> nested = new
                GenericHolderRecord<>(field, 42L);
        return new NestedGenericHolderFieldWithMultipleTypeParamsRecord(nested);
    }

    private static NestedGenericHolderSimpleGenericsRecord getNestedGenericHolderSimpleGenericsRecord() {
        SimpleRecord simpleRecord = getSimpleRecord();
        Map<String, SimpleRecord> map = new HashMap<String, SimpleRecord>();
        map.put("A", simpleRecord);
        Map<String, Map<String, SimpleRecord>> mapB = new HashMap<String, Map<String, SimpleRecord>>();
        mapB.put("A", map);
        SimpleGenericsRecord<Integer, List<SimpleRecord>, Map<String, SimpleRecord>> simpleGenericsRecord =
                new SimpleGenericsRecord<Integer, List<SimpleRecord>, Map<String, SimpleRecord>>(42, 42,
                        singletonList(singletonList(simpleRecord)), mapB);
        GenericHolderRecord<SimpleGenericsRecord<Integer, List<SimpleRecord>, Map<String, SimpleRecord>>> nested =
                new GenericHolderRecord<SimpleGenericsRecord<Integer, List<SimpleRecord>, Map<String, SimpleRecord>>>(simpleGenericsRecord, 42L);

        return new NestedGenericHolderSimpleGenericsRecord(nested);
    }

    private static GenericTreeRecord<String, String> getGenericTreeRecordStrings() {
        return new GenericTreeRecord<String, String>("top", "1",
                new GenericTreeRecord<String, String>("left", "2",
                        new GenericTreeRecord<String, String>("left", "3", null, null), null),
                new GenericTreeRecord<String, String>("right", "4",
                        new GenericTreeRecord<String, String>("left", "5", null, null), null));
    }

    private static NestedSelfReferentialGenericHolderRecord getNestedSelfReferentialGenericHolderRecord() {
        SelfReferentialGenericRecord<Boolean, Long> selfRef1 = new SelfReferentialGenericRecord<Boolean, Long>(true, 33L,
                new SelfReferentialGenericRecord<Long, Boolean>(44L, false, null));
        SelfReferentialGenericRecord<Boolean, Double> selfRef2 = new SelfReferentialGenericRecord<Boolean, Double>(true, 3.14,
                new SelfReferentialGenericRecord<Double, Boolean>(3.42, true, null));
        NestedSelfReferentialGenericRecord<Boolean, Long, Double> nested =
                new NestedSelfReferentialGenericRecord<Boolean, Long, Double>(true, 42L, 44.0, selfRef1, selfRef2);
        return new NestedSelfReferentialGenericHolderRecord(nested);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        RecordCodecProvider automaticProvider = RecordCodecProvider.builder().automatic(true).build();
        CodecRegistry automaticRegistry = getRegistryFromProvider(automaticProvider);

        RecordCodecProvider packageProvider = RecordCodecProvider.builder().register("org.bson.codecs.pojo.entities.records").build();
        CodecRegistry packageRegistry = getRegistryFromProvider(packageProvider);

        List<Object[]> data = new ArrayList<Object[]>();

        for (TestData testData : testCases()) {
            data.add(new Object[]{format("%s", testData.getName()), testData.getRecord(), testData.getRegistry(), testData.getJson() });
            data.add(new Object[]{format("%s [Auto]", testData.getName()), testData.getRecord(), automaticRegistry, testData.getJson() });
            data.add(new Object[]{format("%s [Package]", testData.getName()), testData.getRecord(), packageRegistry, testData.getJson() });
        }
        return data;
    }

    private static class TestData {
        private final String name;
        private final Record record;
        private final CodecRegistry registry;
        private final String json;

        TestData(final String name, final Record record, final CodecRegistry registry, final String json) {
            this.name = name;
            this.record = record;
            this.registry = registry;
            this.json = json;
        }

        public String getName() {
            return name;
        }

        public Object getRecord() {
            return record;
        }

        public CodecRegistry getRegistry() {
            return registry;
        }

        public String getJson() {
            return json;
        }
    }
}
