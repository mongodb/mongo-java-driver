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

import org.bson.codecs.IntegerCodec;
import org.bson.codecs.pojo.annotations.Property;
import org.bson.codecs.pojo.entities.SimpleGenericsModel;
import org.junit.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public final class FieldModelBuilderTest {

    @Test
    @SuppressWarnings("rawtypes")
    public void testFieldMapping() throws NoSuchFieldException {
        Field field = SimpleGenericsModel.class.getDeclaredField("myListField");
        FieldModelBuilder<SimpleGenericsModel> fieldModelBuilder = FieldModel.<SimpleGenericsModel>builder(field);

        assertEquals(field.getName(), fieldModelBuilder.getFieldName());
        assertEquals(field.getName(), fieldModelBuilder.getDocumentFieldName());
        assertTrue(fieldModelBuilder.getAnnotations().isEmpty());
        assertNull(fieldModelBuilder.isDiscriminatorEnabled());
    }

    @Test
    public void testFieldOverrides() throws NoSuchFieldException {
        IntegerCodec integerCodec = new IntegerCodec();
        Field field = SimpleGenericsModel.class.getDeclaredField("myIntegerField");
        FieldModelBuilder<Integer> fieldModelBuilder = FieldModel.<Integer>builder(field)
                .codec(integerCodec)
                .documentFieldName("altDocumentFieldName")
                .annotations(ANNOTATIONS)
                .fieldSerialization(CUSTOM_SERIALIZATION)
                .typeData(TypeData.builder(Integer.class).build())
                .fieldAccessor(FIELD_ACCESSOR)
                .discriminatorEnabled(false);

        assertEquals("myIntegerField", fieldModelBuilder.getFieldName());
        assertEquals("altDocumentFieldName", fieldModelBuilder.getDocumentFieldName());
        assertEquals(integerCodec, fieldModelBuilder.getCodec());
        assertEquals(Integer.class, fieldModelBuilder.getTypeData().getType());
        assertEquals(ANNOTATIONS, fieldModelBuilder.getAnnotations());
        assertEquals(CUSTOM_SERIALIZATION, fieldModelBuilder.getFieldSerialization());
        assertEquals(FIELD_ACCESSOR, fieldModelBuilder.getFieldAccessor());
        assertFalse(fieldModelBuilder.isDiscriminatorEnabled());
    }

    private static final List<Annotation> ANNOTATIONS = Collections.<Annotation>singletonList(
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

    private static final FieldSerialization<Integer> CUSTOM_SERIALIZATION = new FieldSerialization<Integer>() {
        @Override
        public boolean shouldSerialize(final Integer value) {
            return false;
        }
    };

    private static final FieldAccessor<Integer> FIELD_ACCESSOR = new FieldAccessor<Integer>() {
        @Override
        public <S> Integer get(final S instance) {
            return null;
        }
        @Override
        public <S> void set(final S instance, final Integer value) {
        }
    };
}
