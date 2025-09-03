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

import org.bson.BsonType;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.pojo.entities.BsonIdModel;
import org.bson.codecs.pojo.entities.ConventionModel;
import org.bson.codecs.pojo.entities.SimpleModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationBsonPropertyIdModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationBsonRepresentation;
import org.bson.codecs.pojo.entities.conventions.AnnotationCollision;
import org.bson.codecs.pojo.entities.conventions.AnnotationDefaultsModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationNameCollision;
import org.bson.codecs.pojo.entities.conventions.AnnotationWithObjectIdModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationWriteCollision;
import org.bson.codecs.pojo.entities.conventions.BsonIgnoreDuplicatePropertyMultipleTypes;
import org.bson.codecs.pojo.entities.conventions.CreatorConstructorNoKnownIdModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInvalidConstructorModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInvalidMethodModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInvalidMethodReturnTypeModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInvalidMultipleConstructorsModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInvalidMultipleCreatorsModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInvalidMultipleStaticCreatorsModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInvalidTypeConstructorModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInvalidTypeMethodModel;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonList;
import static org.bson.codecs.pojo.Conventions.ANNOTATION_CONVENTION;
import static org.bson.codecs.pojo.Conventions.CLASS_AND_PROPERTY_CONVENTION;
import static org.bson.codecs.pojo.Conventions.DEFAULT_CONVENTIONS;
import static org.bson.codecs.pojo.Conventions.NO_CONVENTIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class ConventionsTest {

    @Test
    public void testDefaultConventions() {
        ClassModel<AnnotationWithObjectIdModel> classModel = ClassModel.builder(AnnotationWithObjectIdModel.class)
                .conventions(DEFAULT_CONVENTIONS).build();

        assertTrue(classModel.useDiscriminator());
        assertEquals("_cls", classModel.getDiscriminatorKey());
        assertEquals("MyAnnotationModel", classModel.getDiscriminator());

        assertEquals(3, classModel.getPropertyModels().size());
        PropertyModel<?> idPropertyModel = classModel.getIdPropertyModel();
        assertNotNull(idPropertyModel);
        assertEquals("customId", idPropertyModel.getName());
        assertEquals("_id", idPropertyModel.getWriteName());
        assertEquals(classModel.getIdPropertyModelHolder().getIdGenerator(), IdGenerators.OBJECT_ID_GENERATOR);

        PropertyModel<?> childPropertyModel = classModel.getPropertyModel("child");
        assertNotNull(childPropertyModel);
        assertFalse(childPropertyModel.useDiscriminator());

        PropertyModel<?> renamedPropertyModel = classModel.getPropertyModel("alternative");
        assertEquals("renamed", renamedPropertyModel.getReadName());
        assertEquals("renamed", renamedPropertyModel.getWriteName());
    }

    @Test
    public void testAnnotationDefaults() {
        ClassModel<AnnotationDefaultsModel> classModel = ClassModel.builder(AnnotationDefaultsModel.class)
                .conventions(singletonList(ANNOTATION_CONVENTION)).build();

        assertTrue(classModel.useDiscriminator());
        assertEquals("_t", classModel.getDiscriminatorKey());
        assertEquals("AnnotationDefaultsModel", classModel.getDiscriminator());

        assertEquals(2, classModel.getPropertyModels().size());
        PropertyModel<?> idPropertyModel = classModel.getIdPropertyModel();
        assertNotNull(idPropertyModel);
        assertEquals("customId", idPropertyModel.getName());
        assertEquals("_id", idPropertyModel.getWriteName());

        PropertyModel<?> childPropertyModel = classModel.getPropertyModel("child");
        assertNotNull(childPropertyModel);
        assertFalse(childPropertyModel.useDiscriminator());
    }

    @Test
    public void testBsonPropertyIdModelModel() {
        ClassModel<AnnotationBsonPropertyIdModel> classModel = ClassModel.builder(AnnotationBsonPropertyIdModel.class)
                .conventions(DEFAULT_CONVENTIONS).build();

        assertFalse(classModel.useDiscriminator());
        assertEquals(1, classModel.getPropertyModels().size());
        assertNull(classModel.getIdPropertyModel());
    }

    @Test
    public void testBsonRepresentation() {
        ClassModel<AnnotationBsonRepresentation> classModel = ClassModel.builder(AnnotationBsonRepresentation.class).build();
        assertEquals(classModel.getPropertyModel("id").getBsonRepresentation(), BsonType.OBJECT_ID);
        assertEquals(classModel.getPropertyModel("parentId").getBsonRepresentation(), BsonType.OBJECT_ID);
        assertNull(classModel.getPropertyModel("friendId").getBsonRepresentation());
        assertNull(classModel.getPropertyModel("age").getBsonRepresentation());
    }

    @Test
    public void testIdGeneratorChoice() {
        ClassModel<AnnotationBsonRepresentation> stringIdObjectRep = ClassModel.builder(AnnotationBsonRepresentation.class).build();
        assertEquals(stringIdObjectRep.getIdPropertyModelHolder().getIdGenerator(), IdGenerators.STRING_ID_GENERATOR);

        ClassModel<ConventionModel> stringIdStringRep = ClassModel.builder(ConventionModel.class).build();
        assertNull(stringIdStringRep.getIdPropertyModelHolder().getIdGenerator());

        ClassModel<BsonIdModel> bsonId = ClassModel.builder(BsonIdModel.class).build();
        assertEquals(bsonId.getIdPropertyModelHolder().getIdGenerator(), IdGenerators.BSON_OBJECT_ID_GENERATOR);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testClassAndFieldConventionDoesNotOverwrite() {
        ClassModelBuilder<SimpleModel> builder = ClassModel.builder(SimpleModel.class)
                .enableDiscriminator(true)
                .discriminatorKey("_cls")
                .discriminator("Simples")
                .conventions(singletonList(CLASS_AND_PROPERTY_CONVENTION))
                .instanceCreatorFactory(() -> null);

        PropertyModelBuilder<Integer> propertyModelBuilder = (PropertyModelBuilder<Integer>) builder.getProperty("integerField");
        propertyModelBuilder.writeName("id")
                .propertySerialization(new PropertyModelSerializationImpl<>())
                .propertyAccessor(new PropertyAccessorTest<>());

        PropertyModelBuilder<String> propertyModelBuilder2 = (PropertyModelBuilder<String>) builder.getProperty("stringField");
        propertyModelBuilder2.writeName("_id")
                .propertySerialization(new PropertyModelSerializationImpl<>())
                .propertyAccessor(new PropertyAccessorTest<>());

        ClassModel<SimpleModel> classModel = builder.idPropertyName("stringField").build();

        assertTrue(classModel.useDiscriminator());
        assertEquals("_cls", classModel.getDiscriminatorKey());
        assertEquals("Simples", classModel.getDiscriminator());

        assertEquals(2, classModel.getPropertyModels().size());
        PropertyModel<?> idPropertyModel = classModel.getIdPropertyModel();
        assertEquals("stringField", idPropertyModel.getName());
        assertEquals("_id", idPropertyModel.getWriteName());
        assertNull(idPropertyModel.useDiscriminator());
    }

    @Test
    public void testAnnotationCollision() {
        assertThrows(CodecConfigurationException.class, () ->
                ClassModel.builder(AnnotationCollision.class).conventions(DEFAULT_CONVENTIONS).build());
    }

    @Test
    public void testAnnotationWriteCollision() {
        assertThrows(CodecConfigurationException.class, () ->
                ClassModel.builder(AnnotationWriteCollision.class).conventions(DEFAULT_CONVENTIONS).build());
    }

    @Test
    public void testAnnotationNameCollision() {
        assertThrows(CodecConfigurationException.class, () ->
                ClassModel.builder(AnnotationNameCollision.class)
                        .conventions(singletonList(ANNOTATION_CONVENTION)).build());
    }

    @Test
    public void testCreatorInvalidConstructorModel() {
        assertThrows(CodecConfigurationException.class, () ->
                ClassModel.builder(CreatorInvalidConstructorModel.class)
                        .conventions(singletonList(ANNOTATION_CONVENTION)).build());
    }

    @Test
    public void testCreatorInvalidMethodModel() {
        assertThrows(CodecConfigurationException.class, () ->
                ClassModel.builder(CreatorInvalidMethodModel.class)
                        .conventions(singletonList(ANNOTATION_CONVENTION)).build());
    }

    @Test
    public void testCreatorInvalidMultipleConstructorsModel() {
        assertThrows(CodecConfigurationException.class, () ->
                ClassModel.builder(CreatorInvalidMultipleConstructorsModel.class)
                        .conventions(singletonList(ANNOTATION_CONVENTION)).build());
    }

    @Test
    public void testCreatorInvalidMultipleCreatorsModel() {
        assertThrows(CodecConfigurationException.class, () ->
                ClassModel.builder(CreatorInvalidMultipleCreatorsModel.class)
                        .conventions(singletonList(ANNOTATION_CONVENTION)).build());
    }

    @Test
    public void testCreatorInvalidMultipleStaticCreatorsModel() {
        assertThrows(CodecConfigurationException.class, () ->
                ClassModel.builder(CreatorInvalidMultipleStaticCreatorsModel.class)
                        .conventions(singletonList(ANNOTATION_CONVENTION)).build());
    }

    @Test
    public void testCreatorInvalidMethodReturnTypeModel() {
        assertThrows(CodecConfigurationException.class, () ->
                ClassModel.builder(CreatorInvalidMethodReturnTypeModel.class)
                        .conventions(singletonList(ANNOTATION_CONVENTION)).build());
    }

    @Test
    public void testCreatorInvalidTypeConstructorModel() {
        assertThrows(CodecConfigurationException.class, () ->
                ClassModel.builder(CreatorInvalidTypeConstructorModel.class)
                        .conventions(singletonList(ANNOTATION_CONVENTION)).build());
    }

    @Test
    public void testCreatorInvalidTypeMethodModel() {
        assertThrows(CodecConfigurationException.class, () ->
                ClassModel.builder(CreatorInvalidTypeMethodModel.class)
                        .conventions(singletonList(ANNOTATION_CONVENTION)).build());
    }

    @Test
    public void testCreatorConstructorNoKnownIdModel() {
        assertThrows(CodecConfigurationException.class, () ->
                ClassModel.builder(CreatorConstructorNoKnownIdModel.class)
                        .conventions(singletonList(ANNOTATION_CONVENTION)).build());
    }

    @Test
    public void testBsonIgnoreDuplicatePropertyMultipleTypesModel() {
        assertThrows(CodecConfigurationException.class, () ->
                ClassModel.builder(BsonIgnoreDuplicatePropertyMultipleTypes.class)
                        .conventions(NO_CONVENTIONS).build());
    }

    private class PropertyAccessorTest<T> implements PropertyAccessor<T> {

        @Override
        public <S> T get(final S instance) {
            return null;
        }

        @Override
        public <S> void set(final S instance, final T value) {

        }
    }
}
