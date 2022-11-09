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
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.bson.codecs.pojo.entities.ConcreteCollectionsModel;
import org.bson.codecs.pojo.entities.GenericHolderModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderModel;
import org.bson.codecs.pojo.entities.SimpleGenericsModel;
import org.bson.codecs.pojo.entities.SimpleIdModel;
import org.bson.codecs.pojo.entities.UpperBoundsConcreteModel;
import org.bson.codecs.pojo.entities.UpperBoundsModel;
import org.junit.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

@SuppressWarnings("rawtypes")
public final class ClassModelBuilderTest {

    @Test
    public void testDefaults() {
        Class<SimpleGenericsModel> clazz = SimpleGenericsModel.class;
        ClassModelBuilder<SimpleGenericsModel> builder = ClassModel.builder(clazz);
        assertEquals(4, builder.getPropertyModelBuilders().size());
        for (Field field : clazz.getDeclaredFields()) {
            assertEquals(field.getName(), builder.getProperty(field.getName()).getWriteName());
        }

        Map<String, TypeParameterMap> fieldNameToTypeParameterMap = new HashMap<>();
        fieldNameToTypeParameterMap.put("myIntegerField", TypeParameterMap.builder().build());
        fieldNameToTypeParameterMap.put("myGenericField", TypeParameterMap.builder().addIndex(0).build());
        fieldNameToTypeParameterMap.put("myListField", TypeParameterMap.builder().addIndex(0, 1).build());
        fieldNameToTypeParameterMap.put("myMapField", TypeParameterMap.builder().addIndex(0, TypeParameterMap.builder().build())
                .addIndex(1, 2).build());

        assertEquals(fieldNameToTypeParameterMap, builder.getPropertyNameToTypeParameterMap());
        assertEquals(3, builder.getConventions().size());
        assertTrue(builder.getAnnotations().isEmpty());
        assertEquals(clazz, builder.getType());
        assertNull(builder.getIdPropertyName());
        assertFalse(builder.useDiscriminator());
        assertNull(builder.getDiscriminator());
    }

    @Test
    public void testCanReflectObjectClass() {
        Class<Object> clazz = Object.class;
        ClassModelBuilder<Object> builder = ClassModel.builder(clazz);

        assertEquals(0, builder.getPropertyModelBuilders().size());
        assertTrue(builder.getPropertyNameToTypeParameterMap().isEmpty());
        assertEquals(3, builder.getConventions().size());
        assertTrue(builder.getAnnotations().isEmpty());
        assertEquals(clazz, builder.getType());
        assertNull(builder.getIdPropertyName());
        assertFalse(builder.useDiscriminator());
        assertNull(builder.getDiscriminator());
    }

    @Test
    public void testMappedBoundedClasses() {
        ClassModelBuilder<? extends UpperBoundsModel> builder = ClassModel.builder(UpperBoundsModel.class);
        assertEquals(Number.class, builder.getProperty("myGenericField").getTypeData().getType());

        builder = ClassModel.builder(UpperBoundsConcreteModel.class);
        assertEquals(Long.class, builder.getProperty("myGenericField").getTypeData().getType());
    }

    @Test
    public void testNestedGenericHolderModel() {
        ClassModelBuilder<NestedGenericHolderModel> builder =
                ClassModel.builder(NestedGenericHolderModel.class);
        assertEquals(GenericHolderModel.class, builder.getProperty("nested").getTypeData().getType());
        assertEquals(TypeData.builder(GenericHolderModel.class).addTypeParameter(TypeData.builder(String.class).build()).build(),
                builder.getProperty("nested").getTypeData());
    }

    @Test
    public void testFieldsMappedClassTypes() {
        ClassModelBuilder<ConcreteCollectionsModel> builder =
                ClassModel.builder(ConcreteCollectionsModel.class);

        assertEquals(Collection.class, builder.getProperty("collection").getTypeData().getType());
        assertEquals(List.class, builder.getProperty("list").getTypeData().getType());
        assertEquals(LinkedList.class, builder.getProperty("linked").getTypeData().getType());
        assertEquals(Map.class, builder.getProperty("map").getTypeData().getType());
        assertEquals(ConcurrentHashMap.class, builder.getProperty("concurrent").getTypeData().getType());
    }

    @Test
    public void testOverrides() throws NoSuchFieldException {
        ClassModelBuilder<SimpleGenericsModel> builder = ClassModel.builder(SimpleGenericsModel.class)
                .annotations(TEST_ANNOTATIONS)
                .conventions(TEST_CONVENTIONS)
                .discriminatorKey("_cls")
                .discriminator("myColl")
                .enableDiscriminator(true)
                .idPropertyName("myIntegerField")
                .instanceCreatorFactory(TEST_INSTANCE_CREATOR_FACTORY);

        assertEquals(TEST_ANNOTATIONS, builder.getAnnotations());
        assertEquals(TEST_CONVENTIONS, builder.getConventions());
        assertEquals("myIntegerField", builder.getIdPropertyName());
        assertEquals(SimpleGenericsModel.class, builder.getType());
        assertTrue(builder.useDiscriminator());
        assertEquals("_cls", builder.getDiscriminatorKey());
        assertEquals("myColl", builder.getDiscriminator());
        assertEquals(TEST_INSTANCE_CREATOR_FACTORY, builder.getInstanceCreatorFactory());
    }

    @Test
    public void testCanRemoveField() {
        ClassModelBuilder<SimpleGenericsModel> builder = ClassModel.builder(SimpleGenericsModel.class)
                .idPropertyName("ID");
        assertEquals(4, builder.getPropertyModelBuilders().size());
        builder.removeProperty("myIntegerField");
        assertEquals(3, builder.getPropertyModelBuilders().size());

        builder.removeProperty("myIntegerField");
        assertEquals(3, builder.getPropertyModelBuilders().size());
    }

    @Test(expected = CodecConfigurationException.class)
    public void testValidationIdProperty() {
        ClassModel.builder(SimpleGenericsModel.class).idPropertyName("ID").build();
    }

    @Test(expected = CodecConfigurationException.class)
    public void testValidationDuplicateDocumentFieldName() {
        ClassModelBuilder<SimpleGenericsModel> builder = ClassModel.builder(SimpleGenericsModel.class);
        builder.getProperty("myIntegerField").writeName("myGenericField");
        builder.build();
    }

    @Test(expected = CodecConfigurationException.class)
    public void testDifferentTypeIdGenerator() {
        ClassModel.builder(SimpleIdModel.class)
                .idGenerator(new IdGenerator<String>() {
                    @Override
                    public String generate() {
                        return "id";
                    }

                    @Override
                    public Class<String> getType() {
                        return String.class;
                    }
                }).build();
    }

    private static final List<Annotation> TEST_ANNOTATIONS = Collections.singletonList(
            new BsonProperty() {
                @Override
                public Class<? extends Annotation> annotationType() {
                    return BsonProperty.class;
                }

                @Override
                public String value() {
                    return "";
                }

                @Override
                public boolean useDiscriminator() {
                    return true;
                }
            });

    private static final List<Convention> TEST_CONVENTIONS = Collections.singletonList(
            new Convention() {
                @Override
                public void apply(final ClassModelBuilder<?> builder) {
                }
            });

    private static final InstanceCreatorFactory<SimpleGenericsModel> TEST_INSTANCE_CREATOR_FACTORY =
            new InstanceCreatorFactory<SimpleGenericsModel>() {
                @Override
                public InstanceCreator<SimpleGenericsModel> create() {
                    return null;
                }
            };
}
