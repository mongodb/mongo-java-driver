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

import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.pojo.entities.SimpleModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationBsonPropertyIdModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationCollision;
import org.bson.codecs.pojo.entities.conventions.AnnotationDefaultsModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationNameCollision;
import org.bson.codecs.pojo.entities.conventions.AnnotationWithObjectIdModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationWriteCollision;
import org.bson.codecs.pojo.entities.conventions.CreatorInvalidConstructorModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInvalidMethodModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInvalidMethodReturnTypeModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInvalidMultipleConstructorsModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInvalidMultipleCreatorsModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInvalidMultipleStaticCreatorsModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInvalidTypeConstructorModel;
import org.bson.codecs.pojo.entities.conventions.CreatorInvalidTypeMethodModel;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.bson.codecs.pojo.Conventions.ANNOTATION_CONVENTION;
import static org.bson.codecs.pojo.Conventions.CLASS_AND_PROPERTY_CONVENTION;
import static org.bson.codecs.pojo.Conventions.DEFAULT_CONVENTIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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
    @SuppressWarnings("unchecked")
    public void testClassAndFieldConventionDoesNotOverwrite() {
        ClassModelBuilder<SimpleModel> builder = ClassModel.builder(SimpleModel.class)
                .enableDiscriminator(true)
                .discriminatorKey("_cls")
                .discriminator("Simples")
                .conventions(singletonList(CLASS_AND_PROPERTY_CONVENTION))
                .instanceCreatorFactory(new InstanceCreatorFactory<SimpleModel>() {
                    @Override
                    public InstanceCreator<SimpleModel> create() {
                        return null;
                    }
                });

        PropertyModelBuilder<Integer> propertyModelBuilder = (PropertyModelBuilder<Integer>) builder.getProperty("integerField");
        propertyModelBuilder.writeName("id")
                .propertySerialization(new PropertyModelSerializationImpl<Integer>())
                .propertyAccessor(new PropertyAccessorTest<Integer>());

        PropertyModelBuilder<String> propertyModelBuilder2 = (PropertyModelBuilder<String>) builder.getProperty("stringField");
        propertyModelBuilder2.writeName("_id")
                .propertySerialization(new PropertyModelSerializationImpl<String>())
                .propertyAccessor(new PropertyAccessorTest<String>());

        ClassModel<SimpleModel> classModel  = builder.idPropertyName("stringField").build();

        assertTrue(classModel.useDiscriminator());
        assertEquals("_cls", classModel.getDiscriminatorKey());
        assertEquals("Simples", classModel.getDiscriminator());

        assertEquals(2, classModel.getPropertyModels().size());
        PropertyModel<?> idPropertyModel = classModel.getIdPropertyModel();
        assertEquals("stringField", idPropertyModel.getName());
        assertEquals("_id", idPropertyModel.getWriteName());
        assertNull(idPropertyModel.useDiscriminator());
    }

    @Test(expected = CodecConfigurationException.class)
    public void testAnnotationCollision() {
        ClassModel.builder(AnnotationCollision.class).conventions(DEFAULT_CONVENTIONS).build();
    }

    @Test(expected = CodecConfigurationException.class)
    public void testAnnotationWriteCollision() {
        ClassModel.builder(AnnotationWriteCollision.class).conventions(DEFAULT_CONVENTIONS).build();
    }

    @Test(expected = CodecConfigurationException.class)
    public void testAnnotationNameCollision() {
        ClassModel.builder(AnnotationNameCollision.class)
                .conventions(singletonList(ANNOTATION_CONVENTION)).build();
    }

    @Test(expected = CodecConfigurationException.class)
    public void testCreatorInvalidConstructorModel() {
        ClassModel.builder(CreatorInvalidConstructorModel.class)
                .conventions(singletonList(ANNOTATION_CONVENTION)).build();
    }

    @Test(expected = CodecConfigurationException.class)
    public void testCreatorInvalidMethodModel() {
        ClassModel.builder(CreatorInvalidMethodModel.class)
                .conventions(singletonList(ANNOTATION_CONVENTION)).build();
    }

    @Test(expected = CodecConfigurationException.class)
    public void testCreatorInvalidMultipleConstructorsModel() {
        ClassModel.builder(CreatorInvalidMultipleConstructorsModel.class)
                .conventions(singletonList(ANNOTATION_CONVENTION)).build();
    }

    @Test(expected = CodecConfigurationException.class)
    public void testCreatorInvalidMultipleCreatorsModel() {
        ClassModel.builder(CreatorInvalidMultipleCreatorsModel.class)
                .conventions(singletonList(ANNOTATION_CONVENTION)).build();
    }

    @Test(expected = CodecConfigurationException.class)
    public void testCreatorInvalidMultipleStaticCreatorsModel() {
        ClassModel.builder(CreatorInvalidMultipleStaticCreatorsModel.class)
                .conventions(singletonList(ANNOTATION_CONVENTION)).build();
    }

    @Test(expected = CodecConfigurationException.class)
    public void testCreatorInvalidMethodReturnTypeModel() {
        ClassModel.builder(CreatorInvalidMethodReturnTypeModel.class)
                .conventions(singletonList(ANNOTATION_CONVENTION)).build();
    }

    @Test(expected = CodecConfigurationException.class)
    public void testCreatorInvalidTypeConstructorModel() {
        ClassModel.builder(CreatorInvalidTypeConstructorModel.class)
                .conventions(singletonList(ANNOTATION_CONVENTION)).build();
    }

    @Test(expected = CodecConfigurationException.class)
    public void testCreatorInvalidTypeMethodModel() {
        ClassModel.builder(CreatorInvalidTypeMethodModel.class)
                .conventions(singletonList(ANNOTATION_CONVENTION)).build();
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
