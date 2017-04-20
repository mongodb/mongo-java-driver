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

import org.bson.codecs.pojo.annotations.Property;
import org.bson.codecs.pojo.entities.ConcreteCollectionsModel;
import org.bson.codecs.pojo.entities.GenericHolderModel;
import org.bson.codecs.pojo.entities.InvalidMapModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderModel;
import org.bson.codecs.pojo.entities.SimpleGenericsModel;
import org.bson.codecs.pojo.entities.UpperBoundsModel;
import org.bson.codecs.pojo.entities.UpperBoundsSubClassModel;
import org.junit.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
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
        assertEquals(4, builder.getFields().size());
        for (Field field : clazz.getDeclaredFields()) {
            assertEquals(field.getName(), builder.getField(field.getName()).getDocumentFieldName());
        }

        Map<String, TypeParameterMap> fieldNameToTypeParameterMap = new HashMap<String, TypeParameterMap>();
        fieldNameToTypeParameterMap.put("myIntegerField", TypeParameterMap.builder().build());
        fieldNameToTypeParameterMap.put("myGenericField", TypeParameterMap.builder().addIndex(0).build());
        fieldNameToTypeParameterMap.put("myListField", TypeParameterMap.builder().addIndex(0, 1).build());
        fieldNameToTypeParameterMap.put("myMapField", TypeParameterMap.builder().addIndex(1, 2).build());

        assertEquals(fieldNameToTypeParameterMap, builder.getFieldNameToTypeParameterMap());
        assertEquals(2, builder.getConventions().size());
        assertTrue(builder.getAnnotations().isEmpty());
        assertEquals(clazz, builder.getType());
        assertNull(builder.getIdField());
        assertFalse(builder.useDiscriminator());
        assertNull(builder.getDiscriminator());
    }

    @Test
    public void testCanReflectObjectClass() {
        Class<Object> clazz = Object.class;
        ClassModelBuilder<Object> builder = ClassModel.builder(clazz);

        assertEquals(0, builder.getFields().size());
        assertTrue(builder.getFieldNameToTypeParameterMap().isEmpty());
        assertEquals(2, builder.getConventions().size());
        assertTrue(builder.getAnnotations().isEmpty());
        assertEquals(clazz, builder.getType());
        assertNull(builder.getIdField());
        assertFalse(builder.useDiscriminator());
        assertNull(builder.getDiscriminator());
    }

    @Test
    public void testMappedBoundedClasses() {
        ClassModelBuilder<? extends UpperBoundsModel> builder = ClassModel.builder(UpperBoundsModel.class);
        assertEquals(Number.class, builder.getField("myGenericField").getTypeData().getType());

        builder = ClassModel.builder(UpperBoundsSubClassModel.class);
        assertEquals(Long.class, builder.getField("myGenericField").getTypeData().getType());
    }

    @Test
    public void testNestedGenericHolderModel() {
        ClassModelBuilder<NestedGenericHolderModel> builder =
                ClassModel.builder(NestedGenericHolderModel.class);
        assertEquals(GenericHolderModel.class, builder.getField("nested").getTypeData().getType());
        assertEquals(TypeData.builder(GenericHolderModel.class).addTypeParameter(TypeData.builder(String.class).build()).build(),
                builder.getField("nested").getTypeData());
    }

    @Test
    public void testFieldsMappedClassTypes() {
        ClassModelBuilder<ConcreteCollectionsModel> builder =
                ClassModel.builder(ConcreteCollectionsModel.class);

        assertEquals(ArrayList.class, builder.getField("collection").getTypeData().getType());
        assertEquals(ArrayList.class, builder.getField("list").getTypeData().getType());
        assertEquals(LinkedList.class, builder.getField("linked").getTypeData().getType());
        assertEquals(HashMap.class, builder.getField("map").getTypeData().getType());
        assertEquals(ConcurrentHashMap.class, builder.getField("concurrent").getTypeData().getType());
    }

    @Test
    public void testOverrides() throws NoSuchFieldException {
        ClassModelBuilder<SimpleGenericsModel> builder = ClassModel.<SimpleGenericsModel>builder(SimpleGenericsModel.class)
                .annotations(TEST_ANNOTATIONS)
                .conventions(TEST_CONVENTIONS)
                .discriminatorKey("_cls")
                .discriminator("myColl")
                .enableDiscriminator(true)
                .idField("myIntegerField")
                .instanceCreatorFactory(TEST_INSTANCE_CREATOR_FACTORY);

        assertEquals(TEST_ANNOTATIONS, builder.getAnnotations());
        assertEquals(TEST_CONVENTIONS, builder.getConventions());
        assertEquals("myIntegerField", builder.getIdField());
        assertEquals(SimpleGenericsModel.class, builder.getType());
        assertTrue(builder.useDiscriminator());
        assertEquals("_cls", builder.getDiscriminatorKey());
        assertEquals("myColl", builder.getDiscriminator());
        assertEquals(TEST_INSTANCE_CREATOR_FACTORY, builder.getInstanceCreatorFactory());
    }

    @Test
    public void testCanRemoveField() {
        ClassModelBuilder<SimpleGenericsModel> builder = ClassModel.builder(SimpleGenericsModel.class)
                .idField("ID");
        assertEquals(4, builder.getFields().size());
        builder.removeField("myIntegerField");
        assertEquals(3, builder.getFields().size());

        builder.removeField("myIntegerField");
        assertEquals(3, builder.getFields().size());
    }

    @Test(expected = IllegalStateException.class)
    public void testValidationIdField() {
        ClassModel.builder(SimpleGenericsModel.class).idField("ID").build();
    }

    @Test(expected = IllegalStateException.class)
    public void testValidationDuplicateDocumentFieldName() {
        ClassModelBuilder<SimpleGenericsModel> builder = ClassModel.builder(SimpleGenericsModel.class);
        builder.getField("myIntegerField").documentFieldName("myGenericField");
        builder.build();
    }

    @Test(expected = IllegalStateException.class)
    public void testIllegalMapKey() {
        ClassModel.builder(InvalidMapModel.class).build();
    }

    private static final List<Annotation> TEST_ANNOTATIONS = Collections.<Annotation>singletonList(
            new Property() {
                @Override
                public Class<? extends Annotation> annotationType() {
                    return Property.class;
                }

                @Override
                public String name() {
                    return "";
                }

                @Override
                public boolean useDiscriminator() {
                    return true;
                }
            });

    private static final List<Convention> TEST_CONVENTIONS = Collections.<Convention>singletonList(
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
