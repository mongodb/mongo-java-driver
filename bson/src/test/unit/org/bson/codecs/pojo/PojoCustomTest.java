/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo;

import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.entities.AsymmetricalCreatorModel;
import org.bson.codecs.pojo.entities.AsymmetricalIgnoreModel;
import org.bson.codecs.pojo.entities.AsymmetricalModel;
import org.bson.codecs.pojo.entities.ConcreteCollectionsModel;
import org.bson.codecs.pojo.entities.ConstructorNotPublicModel;
import org.bson.codecs.pojo.entities.ConventionModel;
import org.bson.codecs.pojo.entities.ConverterModel;
import org.bson.codecs.pojo.entities.GenericTreeModel;
import org.bson.codecs.pojo.entities.InvalidGetterAndSetterModel;
import org.bson.codecs.pojo.entities.InvalidSetterArgsModel;
import org.bson.codecs.pojo.entities.PrimitivesModel;
import org.bson.codecs.pojo.entities.SimpleEnum;
import org.bson.codecs.pojo.entities.SimpleEnumModel;
import org.bson.codecs.pojo.entities.SimpleModel;
import org.bson.codecs.pojo.entities.SimpleNestedPojoModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationModel;
import org.bson.codecs.pojo.entities.conventions.CreatorConstructorPrimitivesModel;
import org.bson.codecs.pojo.entities.conventions.CreatorConstructorThrowsExceptionModel;
import org.bson.codecs.pojo.entities.conventions.CreatorMethodThrowsExceptionModel;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.bson.codecs.pojo.Conventions.NO_CONVENTIONS;

public final class PojoCustomTest extends PojoTestCase {

    @Test
    public void testRegisterClassModelPreferredOverClass() {
        ClassModel<SimpleModel> classModel = ClassModel.builder(SimpleModel.class).enableDiscriminator(true).build();
        PojoCodecProvider.Builder builder = PojoCodecProvider.builder().automatic(true).register(SimpleModel.class).register(classModel);

        roundTrip(builder, getSimpleModel(), "{_t: 'org.bson.codecs.pojo.entities.SimpleModel', 'integerField': 42,"
                + "'stringField': 'myString'}");
    }

    @Test
    public void testPackageDiscriminator() {
        AnnotationModel model = new AnnotationModel("myId", new AnnotationModel("child", null, null),
                new AnnotationModel("alternative", null, null));

        roundTrip(PojoCodecProvider.builder().register("org.bson.codecs.pojo.entities", "org.bson.codecs.pojo.entities.conventions"), model,
                "{_id: 'myId', _cls: 'MyAnnotationModel', renamed: {_id: 'alternative'}, child: {_id: 'child'}}");
    }

    @Test
    public void testAsymmetricalModel() {
        AsymmetricalModel model = new AsymmetricalModel(42);

        encodesTo(getPojoCodecProviderBuilder(AsymmetricalModel.class), model, "{foo: 42}");
        decodesTo(getPojoCodecProviderBuilder(AsymmetricalModel.class), "{bar: 42}", model);
    }

    @Test
    public void testAsymmetricalCreatorModel() {
        AsymmetricalCreatorModel model = new AsymmetricalCreatorModel("Foo", "Bar");

        encodesTo(getPojoCodecProviderBuilder(AsymmetricalCreatorModel.class), model, "{baz: 'FooBar'}");
        decodesTo(getPojoCodecProviderBuilder(AsymmetricalCreatorModel.class), "{a: 'Foo', b: 'Bar'}", model);
    }

    @Test
    public void testAsymmetricalIgnoreModel() {
        AsymmetricalIgnoreModel encode = new AsymmetricalIgnoreModel("property", "getter", "setter", "getterAndSetter");
        AsymmetricalIgnoreModel decoded = new AsymmetricalIgnoreModel();
        decoded.setGetterIgnored("getter");

        encodesTo(getPojoCodecProviderBuilder(AsymmetricalIgnoreModel.class), encode, "{'setterIgnored': 'setter'}");
        decodesTo(getPojoCodecProviderBuilder(AsymmetricalIgnoreModel.class),
                "{'propertyIgnored': 'property', 'getterIgnored': 'getter', 'setterIgnored': 'setter', "
                        + "'getterAndSetterIgnored': 'getterAndSetter'}", decoded);
    }

    @Test
    public void testConventionsEmpty() {
        ClassModelBuilder<ConventionModel> classModel = ClassModel.builder(ConventionModel.class).conventions(NO_CONVENTIONS);
        ClassModelBuilder<SimpleModel> nestedClassModel = ClassModel.builder(SimpleModel.class).conventions(NO_CONVENTIONS);

        roundTrip(getPojoCodecProviderBuilder(classModel, nestedClassModel), getConventionModel(),
                "{'myFinalField': 10, 'myIntField': 10, 'customId': 'id',"
                        + "'child': {'myFinalField': 10, 'myIntField': 10, 'customId': 'child',"
                        + "          'simpleModel': {'integerField': 42, 'stringField': 'myString' } } }");
    }

    @Test
    public void testConventionsCustom() {
        List<Convention> conventions = Collections.<Convention>singletonList(
                new Convention() {
                    @Override
                    public void apply(final ClassModelBuilder<?> classModelBuilder) {
                        for (PropertyModelBuilder<?> fieldModelBuilder : classModelBuilder.getPropertyModelBuilders()) {
                            fieldModelBuilder.discriminatorEnabled(false);
                            fieldModelBuilder.readName(
                                    fieldModelBuilder.getName()
                                            .replaceAll("([^_A-Z])([A-Z])", "$1_$2").toLowerCase());
                            fieldModelBuilder.writeName(
                                    fieldModelBuilder.getName()
                                            .replaceAll("([^_A-Z])([A-Z])", "$1_$2").toLowerCase());
                        }
                        if (classModelBuilder.getProperty("customId") != null) {
                            classModelBuilder.idPropertyName("customId");
                        }
                        classModelBuilder.enableDiscriminator(true);
                        classModelBuilder.discriminatorKey("_cls");
                        classModelBuilder.discriminator(classModelBuilder.getType().getSimpleName()
                                .replaceAll("([^_A-Z])([A-Z])", "$1_$2").toLowerCase());
                    }
                });

        ClassModelBuilder<ConventionModel> classModel = ClassModel.builder(ConventionModel.class).conventions(conventions);
        ClassModelBuilder<SimpleModel> nestedClassModel = ClassModel.builder(SimpleModel.class).conventions(conventions);

        roundTrip(getPojoCodecProviderBuilder(classModel, nestedClassModel), getConventionModel(),
                "{ '_id': 'id', '_cls': 'convention_model', 'my_final_field': 10, 'my_int_field': 10,"
                        + "'child': { '_id': 'child', 'my_final_field': 10, 'my_int_field': 10, "
                        + "           'simple_model': {'integer_field': 42, 'string_field': 'myString' } } }");
    }

    @Test
    public void testEnumSupport() {
        roundTrip(getPojoCodecProviderBuilder(SimpleEnumModel.class), new SimpleEnumModel(SimpleEnum.BRAVO), "{ 'myEnum': 'BRAVO' }");
    }

    @Test
    public void testEnumSupportWithCustomCodec() {
        CodecRegistry registry = fromRegistries(getCodecRegistry(getPojoCodecProviderBuilder(SimpleEnumModel.class)),
                fromCodecs(new SimpleEnumCodec()));
        roundTrip(registry, new SimpleEnumModel(SimpleEnum.BRAVO), "{ 'myEnum': 1 }");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCustomCodec() {
        ObjectId id = new ObjectId();
        ConverterModel model = new ConverterModel(id.toHexString(), "myName");

        ClassModelBuilder<ConverterModel> classModel = ClassModel.builder(ConverterModel.class);
        PropertyModelBuilder<String> idPropertyModelBuilder = (PropertyModelBuilder<String>) classModel.getProperty("id");
        idPropertyModelBuilder.codec(new StringToObjectIdCodec());

        roundTrip(getPojoCodecProviderBuilder(classModel), model,
                format("{'_id': {'$oid': '%s'}, 'name': 'myName'}", id.toHexString()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCustomPropertySerializer() {
        SimpleModel model = getSimpleModel();
        model.setIntegerField(null);
        ClassModelBuilder<SimpleModel> classModel = ClassModel.builder(SimpleModel.class);
        ((PropertyModelBuilder<Integer>) classModel.getProperty("integerField"))
                .propertySerialization(new PropertySerialization<Integer>() {
                    @Override
                    public boolean shouldSerialize(final Integer value) {
                        return true;
                    }
                });

        roundTrip(getPojoCodecProviderBuilder(classModel), model, "{'integerField': null, 'stringField': 'myString'}");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCanHandleNullValuesForNestedModels() {
        SimpleNestedPojoModel model = getSimpleNestedPojoModel();
        model.setSimple(null);
        ClassModelBuilder<SimpleNestedPojoModel> classModel = ClassModel.builder(SimpleNestedPojoModel.class);
        ((PropertyModelBuilder<SimpleModel>) classModel.getProperty("simple"))
                .propertySerialization(new PropertySerialization<SimpleModel>() {
                    @Override
                    public boolean shouldSerialize(final SimpleModel value) {
                        return true;
                    }
                });
        ClassModelBuilder<SimpleModel> classModelSimple = ClassModel.builder(SimpleModel.class);

        roundTrip(getPojoCodecProviderBuilder(classModel, classModelSimple), model, "{'simple': null}");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCanHandleNullValuesForCollectionsAndMaps() {
        ConcreteCollectionsModel model = getConcreteCollectionsModel();
        model.setCollection(null);
        model.setMap(null);

        ClassModelBuilder<ConcreteCollectionsModel> classModel =
                ClassModel.builder(ConcreteCollectionsModel.class);
        ((PropertyModelBuilder<Collection<Integer>>) classModel.getProperty("collection"))
                .propertySerialization(new PropertySerialization<Collection<Integer>>() {
                    @Override
                    public boolean shouldSerialize(final Collection<Integer> value) {
                        return true;
                    }
                });
        ((PropertyModelBuilder<Map<String, Double>>) classModel.getProperty("map"))
                .propertySerialization(new PropertySerialization<Map<String, Double>>() {
                    @Override
                    public boolean shouldSerialize(final Map<String, Double> value) {
                        return true;
                    }
                });

        roundTrip(getPojoCodecProviderBuilder(classModel), model,
                "{'collection': null, 'list': [4, 5, 6], 'linked': [7, 8, 9], 'map': null,"
                        + "'concurrent': {'D': 4.4, 'E': 5.5, 'F': 6.6}}");
    }

    @Test
    public void testCanHandleExtraData() {
        decodesTo(getCodec(SimpleModel.class), "{'integerField': 42,  'stringField': 'myString', 'extraFieldA': 1, 'extraFieldB': 2}",
                getSimpleModel());
    }

    @Test
    public void testDataCanHandleMissingData() {
        SimpleModel model = getSimpleModel();
        model.setIntegerField(null);

        decodesTo(getCodec(SimpleModel.class), "{'_t': 'SimpleModel', 'stringField': 'myString'}", model);
    }

    @Test(expected = CodecConfigurationException.class)
    public void testConstructorNotPublicModel() {
        decodingShouldFail(getCodec(ConstructorNotPublicModel.class), "{'integerField': 99}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testDataUnknownClass() {
        ClassModel<SimpleModel> classModel = ClassModel.builder(SimpleModel.class).enableDiscriminator(true).build();
        decodingShouldFail(getCodec(PojoCodecProvider.builder().register(classModel), SimpleModel.class), "{'_t': 'FakeModel'}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testInvalidTypeForField() {
        decodingShouldFail(getCodec(SimpleModel.class), "{'_t': 'SimpleModel', 'stringField': 123}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testInvalidTypeForPrimitiveField() {
        decodingShouldFail(getCodec(PrimitivesModel.class), "{ '_t': 'PrimitivesModel', 'myBoolean': null}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testInvalidTypeForModelField() {
        decodingShouldFail(getCodec(SimpleNestedPojoModel.class), "{ '_t': 'SimpleNestedPojoModel', 'simple': 123}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testInvalidDiscriminatorInNestedModel() {
        decodingShouldFail(getCodec(SimpleNestedPojoModel.class), "{ '_t': 'SimpleNestedPojoModel',"
                + "'simple': {'_t': 'FakeModel', 'integerField': 42, 'stringField': 'myString'}}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testCannotEncodeUnspecializedClasses() {
        CodecRegistry registry = fromProviders(getPojoCodecProviderBuilder(GenericTreeModel.class).build());
        encode(registry.get(GenericTreeModel.class), getGenericTreeModel());
    }

    @Test(expected = CodecConfigurationException.class)
    public void testCannotDecodeUnspecializedClasses() {
        decodingShouldFail(getCodec(GenericTreeModel.class),
                "{'field1': 'top', 'field2': 1, "
                        + "'left': {'field1': 'left', 'field2': 2, 'left': {'field1': 'left', 'field2': 3}}, "
                        + "'right': {'field1': 'right', 'field2': 4, 'left': {'field1': 'left', 'field2': 5}}}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testBsonCreatorPrimitivesAndNullValues() {
        decodingShouldFail(getCodec(CreatorConstructorPrimitivesModel.class), "{intField: 100,  stringField: 'test'}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testCreatorMethodThrowsExceptionModel() {
        decodingShouldFail(getCodec(CreatorMethodThrowsExceptionModel.class),
                "{'integerField': 10, 'stringField': 'eleven', 'longField': {$numberLong: '12'}}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testCreatorConstructorThrowsExceptionModel() {
        decodingShouldFail(getCodec(CreatorConstructorThrowsExceptionModel.class), "{}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testInvalidSetterModel() {
        decodingShouldFail(getCodec(InvalidSetterArgsModel.class), "{'integerField': 42, 'stringField': 'myString'}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testInvalidGetterAndSetterModelEncoding() {
        InvalidGetterAndSetterModel model = new InvalidGetterAndSetterModel(42, "myString");
        roundTrip(getPojoCodecProviderBuilder(InvalidGetterAndSetterModel.class), model, "{'integerField': 42, 'stringField': 'myString'}");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testInvalidGetterAndSetterModelDecoding() {
        decodingShouldFail(getCodec(InvalidGetterAndSetterModel.class), "{'integerField': 42, 'stringField': 'myString'}");
    }
}
