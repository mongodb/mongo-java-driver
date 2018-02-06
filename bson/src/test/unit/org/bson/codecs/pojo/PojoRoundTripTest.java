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
import org.bson.codecs.pojo.entities.AbstractInterfaceModel;
import org.bson.codecs.pojo.entities.CollectionNestedPojoModel;
import org.bson.codecs.pojo.entities.CollectionSpecificReturnTypeCreatorModel;
import org.bson.codecs.pojo.entities.CollectionSpecificReturnTypeModel;
import org.bson.codecs.pojo.entities.ConcreteAndNestedAbstractInterfaceModel;
import org.bson.codecs.pojo.entities.ConcreteCollectionsModel;
import org.bson.codecs.pojo.entities.ConcreteInterfaceGenericModel;
import org.bson.codecs.pojo.entities.ConcreteStandAloneAbstractInterfaceModel;
import org.bson.codecs.pojo.entities.ContainsAlternativeMapAndCollectionModel;
import org.bson.codecs.pojo.entities.ConventionModel;
import org.bson.codecs.pojo.entities.FieldAndPropertyTypeMismatchModel;
import org.bson.codecs.pojo.entities.GenericHolderModel;
import org.bson.codecs.pojo.entities.GenericTreeModel;
import org.bson.codecs.pojo.entities.InterfaceBasedModel;
import org.bson.codecs.pojo.entities.InterfaceModelImpl;
import org.bson.codecs.pojo.entities.InterfaceUpperBoundsModelAbstractImpl;
import org.bson.codecs.pojo.entities.MultipleBoundsModel;
import org.bson.codecs.pojo.entities.MultipleLevelGenericModel;
import org.bson.codecs.pojo.entities.NestedFieldReusingClassTypeParameter;
import org.bson.codecs.pojo.entities.NestedGenericHolderFieldWithMultipleTypeParamsModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderMapModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderSimpleGenericsModel;
import org.bson.codecs.pojo.entities.NestedGenericTreeModel;
import org.bson.codecs.pojo.entities.NestedMultipleLevelGenericModel;
import org.bson.codecs.pojo.entities.NestedReusedGenericsModel;
import org.bson.codecs.pojo.entities.NestedSelfReferentialGenericHolderModel;
import org.bson.codecs.pojo.entities.NestedSelfReferentialGenericModel;
import org.bson.codecs.pojo.entities.PrimitivesModel;
import org.bson.codecs.pojo.entities.PropertyReusingClassTypeParameter;
import org.bson.codecs.pojo.entities.PropertySelectionModel;
import org.bson.codecs.pojo.entities.PropertyWithMultipleTypeParamsModel;
import org.bson.codecs.pojo.entities.ReusedGenericsModel;
import org.bson.codecs.pojo.entities.SelfReferentialGenericModel;
import org.bson.codecs.pojo.entities.ShapeHolderModel;
import org.bson.codecs.pojo.entities.ShapeModelAbstract;
import org.bson.codecs.pojo.entities.ShapeModelCircle;
import org.bson.codecs.pojo.entities.ShapeModelRectangle;
import org.bson.codecs.pojo.entities.SimpleEnum;
import org.bson.codecs.pojo.entities.SimpleEnumModel;
import org.bson.codecs.pojo.entities.SimpleGenericsModel;
import org.bson.codecs.pojo.entities.SimpleModel;
import org.bson.codecs.pojo.entities.SimpleNestedPojoModel;
import org.bson.codecs.pojo.entities.UpperBoundsConcreteModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationBsonPropertyIdModel;
import org.bson.codecs.pojo.entities.conventions.BsonIgnoreInvalidMapModel;
import org.bson.codecs.pojo.entities.conventions.CollectionDiscriminatorAbstractClassesModel;
import org.bson.codecs.pojo.entities.conventions.CollectionDiscriminatorInterfacesModel;
import org.bson.codecs.pojo.entities.conventions.CreatorAllFinalFieldsModel;
import org.bson.codecs.pojo.entities.conventions.CreatorConstructorIdModel;
import org.bson.codecs.pojo.entities.conventions.CreatorConstructorLegacyBsonPropertyModel;
import org.bson.codecs.pojo.entities.conventions.CreatorConstructorModel;
import org.bson.codecs.pojo.entities.conventions.CreatorConstructorRenameModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInSuperClassModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInSuperClassModelImpl;
import org.bson.codecs.pojo.entities.conventions.CreatorMethodModel;
import org.bson.codecs.pojo.entities.conventions.CreatorNoArgsConstructorModel;
import org.bson.codecs.pojo.entities.conventions.CreatorNoArgsMethodModel;
import org.bson.codecs.pojo.entities.conventions.InterfaceModel;
import org.bson.codecs.pojo.entities.conventions.InterfaceModelImplA;
import org.bson.codecs.pojo.entities.conventions.InterfaceModelImplB;
import org.bson.codecs.pojo.entities.conventions.Subclass1Model;
import org.bson.codecs.pojo.entities.conventions.Subclass2Model;
import org.bson.codecs.pojo.entities.conventions.SuperClassModel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
public final class PojoRoundTripTest extends PojoTestCase {

    private final String name;
    private final Object model;
    private final PojoCodecProvider.Builder builder;
    private final String json;

    public PojoRoundTripTest(final String name, final Object model, final String json, final PojoCodecProvider.Builder builder) {
        this.name = name;
        this.model = model;
        this.json = json;
        this.builder = builder;
    }

    @Test
    public void test() {
        roundTrip(builder, model, json);
    }

    private static List<TestData> testCases() {
        List<TestData> data = new ArrayList<TestData>();
        data.add(new TestData("Simple model", getSimpleModel(), PojoCodecProvider.builder().register(SimpleModel.class),
                SIMPLE_MODEL_JSON));

        data.add(new TestData("Property selection model", new PropertySelectionModel(),
                getPojoCodecProviderBuilder(PropertySelectionModel.class),
                "{'finalStringField': 'finalStringField', 'stringField': 'stringField'}"));

        data.add(new TestData("Conventions default", getConventionModel(),
                getPojoCodecProviderBuilder(ConventionModel.class, SimpleModel.class),
                "{'_id': 'id', '_cls': 'AnnotatedConventionModel', 'myFinalField': 10, 'myIntField': 10,"
                        + "'child': {'_id': 'child', 'myFinalField': 10, 'myIntField': 10,"
                        + "'model': {'integerField': 42, 'stringField': 'myString'}}}"));

        data.add(new TestData("BsonIgnore invalid map", new BsonIgnoreInvalidMapModel("myString"),
                getPojoCodecProviderBuilder(BsonIgnoreInvalidMapModel.class),
                "{stringField: 'myString'}"));

        data.add(new TestData("Interfaced based model", new InterfaceModelImpl("a", "b"),
                getPojoCodecProviderBuilder(InterfaceModelImpl.class),
                "{'propertyA': 'a', 'propertyB': 'b'}"));

        data.add(new TestData("Interfaced based model with bound", new InterfaceUpperBoundsModelAbstractImpl("someName",
                new InterfaceModelImpl("a", "b")),
                getPojoCodecProviderBuilder(InterfaceUpperBoundsModelAbstractImpl.class, InterfaceModelImpl.class),
                "{'name': 'someName', 'nestedModel': {'propertyA': 'a', 'propertyB': 'b'}}"));

        data.add(new TestData("Interface concrete and abstract model",
                new ConcreteAndNestedAbstractInterfaceModel("A", new ConcreteAndNestedAbstractInterfaceModel("B",
                        new ConcreteStandAloneAbstractInterfaceModel("C"))),
                getPojoCodecProviderBuilder(InterfaceBasedModel.class, AbstractInterfaceModel.class,
                        ConcreteAndNestedAbstractInterfaceModel.class, ConcreteStandAloneAbstractInterfaceModel.class),
                "{'_t': 'org.bson.codecs.pojo.entities.ConcreteAndNestedAbstractInterfaceModel', 'name': 'A', "
                        + "'child': {'_t': 'org.bson.codecs.pojo.entities.ConcreteAndNestedAbstractInterfaceModel', 'name': 'B', "
                        + "  'child': {'_t': 'org.bson.codecs.pojo.entities.ConcreteStandAloneAbstractInterfaceModel', 'name': 'C'}}}}"));

        data.add(new TestData("Concrete generic interface model", new ConcreteInterfaceGenericModel("someValue"),
                getPojoCodecProviderBuilder(ConcreteInterfaceGenericModel.class), "{propertyA: 'someValue'}"));

        data.add(new TestData("Primitives model", getPrimitivesModel(),
                getPojoCodecProviderBuilder(PrimitivesModel.class),
                "{ 'myBoolean': true, 'myByte': 1, 'myCharacter': '1', 'myDouble': 1.0, 'myFloat': 2.0, 'myInteger': 3, "
                        + "'myLong': { '$numberLong': '5' }, 'myShort': 6}"));

        data.add(new TestData("Concrete collections model", getConcreteCollectionsModel(),
                getPojoCodecProviderBuilder(ConcreteCollectionsModel.class),
                "{'collection': [1, 2, 3], 'list': [4, 5, 6], 'linked': [7, 8, 9], 'map': {'A': 1.1, 'B': 2.2, 'C': 3.3},"
                        + "'concurrent': {'D': 4.4, 'E': 5.5, 'F': 6.6}}"));

        data.add(new TestData("Handling of nulls inside collections", getConcreteCollectionsModelWithNulls(),
                getPojoCodecProviderBuilder(ConcreteCollectionsModel.class),
                "{'collection': [1, null, 3], 'list': [4, null, 6], 'linked': [null, 8, 9], 'map': {'A': 1.1, 'B': null, 'C': 3.3}}"));

        data.add(new TestData("Concrete specific return collection type model through BsonCreator",
                new CollectionSpecificReturnTypeCreatorModel(Arrays.asList("foo", "bar")),
                getPojoCodecProviderBuilder(CollectionSpecificReturnTypeCreatorModel.class),
                "{'properties': ['foo', 'bar']}"));

        data.add(new TestData("Concrete specific return collection type model through getter and setter",
                new CollectionSpecificReturnTypeModel(Arrays.asList("foo", "bar")),
                getPojoCodecProviderBuilder(CollectionSpecificReturnTypeModel.class),
                "{'properties': ['foo', 'bar']}"));

        data.add(new TestData("Concrete specific return collection type model", getConcreteCollectionsModel(),
                getPojoCodecProviderBuilder(ConcreteCollectionsModel.class),
                "{'collection': [1, 2, 3], 'list': [4, 5, 6], 'linked': [7, 8, 9], 'map': {'A': 1.1, 'B': 2.2, 'C': 3.3},"
                        + "'concurrent': {'D': 4.4, 'E': 5.5, 'F': 6.6}}"));

        data.add(new TestData("Nested simple", getSimpleNestedPojoModel(),
                getPojoCodecProviderBuilder(SimpleNestedPojoModel.class, SimpleModel.class),
                "{'simple': " + SIMPLE_MODEL_JSON + "}"));

        data.add(new TestData("Nested collection", getCollectionNestedPojoModel(),
                getPojoCodecProviderBuilder(CollectionNestedPojoModel.class, SimpleModel.class),
                "{ 'listSimple': [" + SIMPLE_MODEL_JSON + "],"
                        + "'listListSimple': [[" + SIMPLE_MODEL_JSON + "]],"
                        + "'setSimple': [" + SIMPLE_MODEL_JSON + "],"
                        + "'setSetSimple': [[" + SIMPLE_MODEL_JSON + "]],"
                        + "'mapSimple': {'s': " + SIMPLE_MODEL_JSON + "},"
                        + "'mapMapSimple': {'ms': {'s': " + SIMPLE_MODEL_JSON + "}},"
                        + "'mapListSimple': {'ls': [" + SIMPLE_MODEL_JSON + "]},"
                        + "'mapListMapSimple': {'lm': [{'s': " + SIMPLE_MODEL_JSON + "}]},"
                        + "'mapSetSimple': {'s': [" + SIMPLE_MODEL_JSON + "]},"
                        + "'listMapSimple': [{'s': " + SIMPLE_MODEL_JSON + "}],"
                        + "'listMapListSimple': [{'ls': [" + SIMPLE_MODEL_JSON + "]}],"
                        + "'listMapSetSimple': [{'s': [" + SIMPLE_MODEL_JSON + "]}],"
                        + "}"));

        data.add(new TestData("Nested collection", getCollectionNestedPojoModelWithNulls(),
                getPojoCodecProviderBuilder(CollectionNestedPojoModel.class, SimpleModel.class),
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

        data.add(new TestData("Nested generic holder", getNestedGenericHolderModel(),
                getPojoCodecProviderBuilder(NestedGenericHolderModel.class, GenericHolderModel.class),
                "{'nested': {'myGenericField': 'generic', 'myLongField': {'$numberLong': '1'}}}"));

        data.add(new TestData("Nested generic holder map", getNestedGenericHolderMapModel(),
                getPojoCodecProviderBuilder(NestedGenericHolderMapModel.class,
                        GenericHolderModel.class, SimpleGenericsModel.class, SimpleModel.class),
                "{ 'nested': { 'myGenericField': {'s': " + SIMPLE_MODEL_JSON + "}, 'myLongField': {'$numberLong': '1'}}}"));

        data.add(new TestData("Nested reused generic", getNestedReusedGenericsModel(),
                getPojoCodecProviderBuilder(NestedReusedGenericsModel.class, ReusedGenericsModel.class, SimpleModel.class),
                "{ 'nested':{ 'field1':{ '$numberLong':'1' }, 'field2':[" + SIMPLE_MODEL_JSON + "], "
                        + "'field3':'field3', 'field4':42, 'field5':'field5', 'field6':[" + SIMPLE_MODEL_JSON + ", "
                        + SIMPLE_MODEL_JSON + "], 'field7':{ '$numberLong':'2' }, 'field8':'field8' } }"));


        data.add(new TestData("Nested generic holder with multiple types", getNestedGenericHolderFieldWithMultipleTypeParamsModel(),
                getPojoCodecProviderBuilder(NestedGenericHolderFieldWithMultipleTypeParamsModel.class,
                        PropertyWithMultipleTypeParamsModel.class, SimpleGenericsModel.class, GenericHolderModel.class),
                "{'nested': {'myGenericField': {_t: 'PropertyWithMultipleTypeParamsModel', "
                        + "'simpleGenericsModel': {_t: 'org.bson.codecs.pojo.entities.SimpleGenericsModel', 'myIntegerField': 42, "
                        + "'myGenericField': {'$numberLong': '101'}, 'myListField': ['B', 'C'], 'myMapField': {'D': 2, 'E': 3, 'F': 4 }}},"
                        + "'myLongField': {'$numberLong': '42'}}}"));


        data.add(new TestData("Nested generic tree", new NestedGenericTreeModel(42, getGenericTreeModel()),
                getPojoCodecProviderBuilder(NestedGenericTreeModel.class, GenericTreeModel.class),
                "{'intField': 42, 'nested': {'field1': 'top', 'field2': 1, "
                        + "'left': {'field1': 'left', 'field2': 2, 'left': {'field1': 'left', 'field2': 3}}, "
                        + "'right': {'field1': 'right', 'field2': 4, 'left': {'field1': 'left', 'field2': 5}}}}"));

        data.add(new TestData("Nested multiple level",
                new NestedMultipleLevelGenericModel(42, new MultipleLevelGenericModel<String>("string", getGenericTreeModel())),
                getPojoCodecProviderBuilder(NestedMultipleLevelGenericModel.class, MultipleLevelGenericModel.class, GenericTreeModel.class),
                "{'intField': 42, 'nested': {'stringField': 'string', 'nested': {'field1': 'top', 'field2': 1, "
                        + "'left': {'field1': 'left', 'field2': 2, 'left': {'field1': 'left', 'field2': 3}}, "
                        + "'right': {'field1': 'right', 'field2': 4, 'left': {'field1': 'left', 'field2': 5}}}}}"));

        data.add(new TestData("Nested Generics holder", getNestedGenericHolderSimpleGenericsModel(),
                getPojoCodecProviderBuilder(NestedGenericHolderSimpleGenericsModel.class, GenericHolderModel.class,
                        SimpleGenericsModel.class, SimpleModel.class),
                "{'nested': {'myGenericField': {'myIntegerField': 42, 'myGenericField': 42,"
                        + "                           'myListField': [[" + SIMPLE_MODEL_JSON + "]], "
                        + "                           'myMapField': {'A': {'A': " + SIMPLE_MODEL_JSON + "}}},"
                        + "         'myLongField': {'$numberLong': '42' }}}"));

        data.add(new TestData("Nested property reusing type parameter",
                new NestedFieldReusingClassTypeParameter(new PropertyReusingClassTypeParameter<String>(getGenericTreeModelStrings())),
                getPojoCodecProviderBuilder(NestedFieldReusingClassTypeParameter.class, PropertyReusingClassTypeParameter.class,
                        GenericTreeModel.class),
                "{'nested': {'tree': {'field1': 'top', 'field2': '1', "
                        + "'left': {'field1': 'left', 'field2': '2', 'left': {'field1': 'left', 'field2': '3'}}, "
                        + "'right': {'field1': 'right', 'field2': '4', 'left': {'field1': 'left', 'field2': '5'}}}}}"));

        data.add(new TestData("Abstract shape model - circle",
                new ShapeHolderModel(getShapeModelCircle()), getPojoCodecProviderBuilder(ShapeModelAbstract.class,
                ShapeModelCircle.class, ShapeModelRectangle.class, ShapeHolderModel.class),
                "{'shape': {'_t': 'org.bson.codecs.pojo.entities.ShapeModelCircle', 'color': 'orange', 'radius': 4.2}}"));

        data.add(new TestData("Abstract shape model - rectangle",
                new ShapeHolderModel(getShapeModelRectangle()), getPojoCodecProviderBuilder(ShapeModelAbstract.class,
                ShapeModelCircle.class, ShapeModelRectangle.class, ShapeHolderModel.class),
                "{'shape': {'_t': 'org.bson.codecs.pojo.entities.ShapeModelRectangle', 'color': 'green', 'width': 22.1, 'height': "
                        + "105.0}}}"));

        data.add(new TestData("Upper bounds",
                new UpperBoundsConcreteModel(1L), getPojoCodecProviderBuilder(UpperBoundsConcreteModel.class),
                "{'myGenericField': {'$numberLong': '1'}}"));

        data.add(new TestData("Multiple bounds", getMultipleBoundsModel(), getPojoCodecProviderBuilder(MultipleBoundsModel.class),
                "{'level1' : 2.2, 'level2': [1, 2, 3], 'level3': {key: 'value'}}"));

        data.add(new TestData("Self referential", getNestedSelfReferentialGenericHolderModel(),
                getPojoCodecProviderBuilder(NestedSelfReferentialGenericHolderModel.class, NestedSelfReferentialGenericModel.class,
                        SelfReferentialGenericModel.class),
                "{'nested': { 't': true, 'v': {'$numberLong': '42'}, 'z': 44.0, "
                        + "'selfRef1': {'t': true, 'v': {'$numberLong': '33'}, 'child': {'t': {'$numberLong': '44'}, 'v': false}}, "
                        + "'selfRef2': {'t': true, 'v': 3.14, 'child': {'t': 3.42, 'v': true}}}}"));

        data.add(new TestData("Creator constructor", new CreatorConstructorModel(asList(10, 11), "twelve", 13),
                getPojoCodecProviderBuilder(CreatorConstructorModel.class),
                "{'integersField': [10, 11], 'stringField': 'twelve', 'longField': {$numberLong: '13'}}"));

        data.add(new TestData("Creator constructor with legacy BsonProperty using name",
                new CreatorConstructorLegacyBsonPropertyModel(asList(10, 11), "twelve", 13),
                getPojoCodecProviderBuilder(CreatorConstructorLegacyBsonPropertyModel.class),
                "{'integersField': [10, 11], 'stringField': 'twelve', 'longField': {$numberLong: '13'}}"));

        data.add(new TestData("Creator constructor with rename", new CreatorConstructorRenameModel(asList(10, 11), "twelve", 13),
            getPojoCodecProviderBuilder(CreatorConstructorRenameModel.class),
            "{'integerList': [10, 11], 'stringField': 'twelve', 'longField': {$numberLong: '13'}}"));

        data.add(new TestData("Creator constructor with ID", new CreatorConstructorIdModel("1234-34567-890", asList(10, 11), "twelve", 13),
            getPojoCodecProviderBuilder(CreatorConstructorIdModel.class),
            "{'_id': '1234-34567-890', 'integersField': [10, 11], 'stringField': 'twelve', 'longField': {$numberLong: '13'}}"));

        data.add(new TestData("Creator no-args constructor", new CreatorNoArgsConstructorModel(40, "one", 42),
                getPojoCodecProviderBuilder(CreatorNoArgsConstructorModel.class),
                "{'integerField': 40, 'stringField': 'one', 'longField': {$numberLong: '42'}}"));

        data.add(new TestData("Creator method", new CreatorMethodModel(30, "two", 32),
                getPojoCodecProviderBuilder(CreatorMethodModel.class),
                "{'integerField': 30, 'stringField': 'two', 'longField': {$numberLong: '32'}}"));

        data.add(new TestData("Creator method", CreatorMethodModel.create(30),
                getPojoCodecProviderBuilder(CreatorMethodModel.class),
                "{'integerField': 30, 'longField': {$numberLong: '0'}}"));

        data.add(new TestData("Creator no-args method", new CreatorNoArgsMethodModel(10, "one", 11),
                getPojoCodecProviderBuilder(CreatorNoArgsMethodModel.class),
                "{'integerField': 10, 'stringField': 'one', 'longField': {$numberLong: '11'}}"));

        data.add(new TestData("Creator all final", new CreatorAllFinalFieldsModel("pId", "Ada", "Lovelace"),
                getPojoCodecProviderBuilder(CreatorAllFinalFieldsModel.class),
                "{'_id': 'pId', '_t': 'org.bson.codecs.pojo.entities.conventions.CreatorAllFinalFieldsModel', "
                        + "'firstName': 'Ada', 'lastName': 'Lovelace'}"));

        data.add(new TestData("Creator all final with nulls", new CreatorAllFinalFieldsModel("pId", "Ada", null),
                getPojoCodecProviderBuilder(CreatorAllFinalFieldsModel.class),
                "{'_id': 'pId', '_t': 'org.bson.codecs.pojo.entities.conventions.CreatorAllFinalFieldsModel', 'firstName': 'Ada'}"));

        data.add(new TestData("Can handle custom Maps and Collections",
                new ContainsAlternativeMapAndCollectionModel(BsonDocument.parse("{customList: [1,2,3], customMap: {'field': 'value'}}")),
                getPojoCodecProviderBuilder(ContainsAlternativeMapAndCollectionModel.class),
                "{customList: [1,2,3], customMap: {'field': 'value'}}"));

        data.add(new TestData("Collection of discriminators abstract classes", new CollectionDiscriminatorAbstractClassesModel().setList(
                asList(new Subclass1Model().setName("abc").setValue(true), new Subclass2Model().setInteger(234).setValue(false))).setMap(
                Collections.singletonMap("key", new Subclass2Model().setInteger(123).setValue(true))),
                getPojoCodecProviderBuilder(CollectionDiscriminatorAbstractClassesModel.class, SuperClassModel.class, Subclass1Model.class,
                        Subclass2Model.class),
                "{list: [{_t: 'org.bson.codecs.pojo.entities.conventions.Subclass1Model', value: true, name: 'abc'},"
                        + "{_t: 'org.bson.codecs.pojo.entities.conventions.Subclass2Model', value: false, integer: 234}],"
                        + "map: {key: {_t: 'org.bson.codecs.pojo.entities.conventions.Subclass2Model', value: true, integer: 123}}}"));

        data.add(new TestData("Collection of discriminators interfaces", new CollectionDiscriminatorInterfacesModel().setList(
                asList(new InterfaceModelImplA().setName("abc").setValue(true),
                       new InterfaceModelImplB().setInteger(234).setValue(false))).setMap(
                Collections.<String, InterfaceModel>singletonMap("key", new InterfaceModelImplB().setInteger(123).setValue(true))),
                getPojoCodecProviderBuilder(CollectionDiscriminatorInterfacesModel.class, InterfaceModelImplA.class,
                        InterfaceModelImplB.class, InterfaceModel.class),
                "{list: [{_t: 'org.bson.codecs.pojo.entities.conventions.InterfaceModelImplA', value: true, name: 'abc'},"
                        + "{_t: 'org.bson.codecs.pojo.entities.conventions.InterfaceModelImplB', value: false, integer: 234}],"
                        + "map: {key: {_t: 'org.bson.codecs.pojo.entities.conventions.InterfaceModelImplB', value: true, integer: 123}}}"));

        data.add(new TestData("Creator in super class factory method",
                CreatorInSuperClassModel.newInstance("a", "b"),
                getPojoCodecProviderBuilder(CreatorInSuperClassModelImpl.class),
                "{'propertyA': 'a', 'propertyB': 'b'}"));

        data.add(new TestData("Primitive field type doesn't match private property",
                new FieldAndPropertyTypeMismatchModel("foo"),
                getPojoCodecProviderBuilder(FieldAndPropertyTypeMismatchModel.class),
                "{'stringField': 'foo'}"));

        data.add(new TestData("Enums support",
                new SimpleEnumModel(SimpleEnum.BRAVO),
                getPojoCodecProviderBuilder(SimpleEnumModel.class),
                "{ 'myEnum': 'BRAVO' }"));

        data.add(new TestData("AnnotationBsonPropertyIdModel", new AnnotationBsonPropertyIdModel(99L),
                getPojoCodecProviderBuilder(AnnotationBsonPropertyIdModel.class),
                "{'id': {'$numberLong': '99' }}"));
        return data;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> data = new ArrayList<Object[]>();

        for (TestData testData : testCases()) {
            data.add(new Object[]{format("%s", testData.getName()), testData.getModel(), testData.getJson(), testData.getBuilder()});
            data.add(new Object[]{format("%s [Auto]", testData.getName()), testData.getModel(), testData.getJson(), AUTOMATIC_BUILDER});
        }
        return data;
    }

    private static final PojoCodecProvider.Builder AUTOMATIC_BUILDER = PojoCodecProvider.builder().automatic(true);

    private static class TestData {
        private final String name;
        private final Object model;
        private final PojoCodecProvider.Builder builder;
        private final String json;

        TestData(final String name, final Object model, final PojoCodecProvider.Builder builder, final String json) {
            this.name = name;
            this.model = model;
            this.builder = builder;
            this.json = json;
        }

        public String getName() {
            return name;
        }

        public Object getModel() {
            return model;
        }

        public PojoCodecProvider.Builder getBuilder() {
            return builder;
        }

        public String getJson() {
            return json;
        }
    }


}
