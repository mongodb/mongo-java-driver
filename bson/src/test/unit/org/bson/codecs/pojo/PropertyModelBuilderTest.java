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
import org.junit.Test;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.bson.codecs.pojo.PojoBuilderHelper.createPropertyModelBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public final class PropertyModelBuilderTest {

    private static final String FIELD_NAME = "myFieldName";
    private static final PropertyMetadata<Integer> PROPERTY_METADATA =
            new PropertyMetadata<Integer>(FIELD_NAME, "MyClass", TypeData.builder(Integer.class).build());

    @Test
    public void testFieldMapping() throws NoSuchFieldException {
        PropertyModelBuilder<Integer> propertyModelBuilder = createPropertyModelBuilder(PROPERTY_METADATA);
        assertEquals(FIELD_NAME, propertyModelBuilder.getPropertyName());
        assertEquals(FIELD_NAME, propertyModelBuilder.getDocumentPropertyName());
        assertTrue(propertyModelBuilder.getAnnotations().isEmpty());
        assertNull(propertyModelBuilder.isDiscriminatorEnabled());
    }

    @Test
    public void testFieldOverrides() throws NoSuchFieldException {
        IntegerCodec codec = new IntegerCodec();
        PropertyModelBuilder<Integer> propertyModelBuilder = createPropertyModelBuilder(PROPERTY_METADATA)
                .codec(codec)
                .documentPropertyName("altDocumentFieldName")
                .annotations(ANNOTATIONS)
                .propertySerialization(CUSTOM_SERIALIZATION)
                .typeData(TypeData.builder(Integer.class).build())
                .propertyAccessor(FIELD_ACCESSOR)
                .discriminatorEnabled(false);

        assertEquals(FIELD_NAME, propertyModelBuilder.getPropertyName());
        assertEquals("altDocumentFieldName", propertyModelBuilder.getDocumentPropertyName());
        assertEquals(codec, propertyModelBuilder.getCodec());
        assertEquals(Integer.class, propertyModelBuilder.getTypeData().getType());
        assertEquals(ANNOTATIONS, propertyModelBuilder.getAnnotations());
        assertEquals(CUSTOM_SERIALIZATION, propertyModelBuilder.getPropertySerialization());
        assertEquals(FIELD_ACCESSOR, propertyModelBuilder.getPropertyAccessor());
        assertFalse(propertyModelBuilder.isDiscriminatorEnabled());
    }

    private static final List<Annotation> ANNOTATIONS = Collections.<Annotation>singletonList(
            new Property() {
                @Override
                public Class<? extends Annotation> annotationType() {
                    return Property.class;
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

    private static final PropertySerialization<Integer> CUSTOM_SERIALIZATION = new PropertySerialization<Integer>() {
        @Override
        public boolean shouldSerialize(final Integer value) {
            return false;
        }
    };

    private static final PropertyAccessor<Integer> FIELD_ACCESSOR = new PropertyAccessor<Integer>() {
        @Override
        public <S> Integer get(final S instance) {
            return null;
        }
        @Override
        public <S> void set(final S instance, final Integer value) {
        }
    };
}
