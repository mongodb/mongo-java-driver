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

import org.bson.codecs.IntegerCodec;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.List;

import static org.bson.codecs.pojo.PojoBuilderHelper.createPropertyModelBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

public final class PropertyModelTest {

    private static final String FIELD_NAME = "myFieldName";
    private static final PropertyMetadata<Integer> PROPERTY_METADATA =
            new PropertyMetadata<>(FIELD_NAME, "MyClass", TypeData.builder(Integer.class).build());

    @Test
    public void testPropertyMapping() throws NoSuchFieldException {
        PropertySerialization<Integer> serializer = new PropertyModelSerializationImpl<>();
        PropertyAccessor<Integer> accessor = new PropertyAccessorImpl<>(PROPERTY_METADATA);
        PropertyModel<Integer> propertyModel = createPropertyModelBuilder(PROPERTY_METADATA)
                .propertySerialization(serializer)
                .propertyAccessor(accessor)
                .build();
        assertEquals(FIELD_NAME, propertyModel.getName());
        assertEquals(FIELD_NAME, propertyModel.getWriteName());
        assertEquals(serializer, propertyModel.getPropertySerialization());
        assertEquals(accessor, propertyModel.getPropertyAccessor());
        assertNull(propertyModel.getCodec());
        assertNull(propertyModel.getCachedCodec());
        assertNull(propertyModel.useDiscriminator());
    }

    @Test
    public void testPropertyOverrides() throws NoSuchFieldException {
        IntegerCodec codec = new IntegerCodec();
        PropertyModel<Integer> propertyModel = createPropertyModelBuilder(PROPERTY_METADATA)
                .codec(codec)
                .writeName("altDocumentFieldName")
                .readAnnotations(ANNOTATIONS)
                .propertySerialization(CUSTOM_SERIALIZATION)
                .typeData(TypeData.builder(Integer.class).build())
                .propertyAccessor(FIELD_ACCESSOR)
                .discriminatorEnabled(false)
                .build();

        assertEquals(FIELD_NAME, propertyModel.getName());
        assertEquals("altDocumentFieldName", propertyModel.getWriteName());
        assertEquals(codec, propertyModel.getCodec());
        assertEquals(codec, propertyModel.getCachedCodec());
        assertEquals(Integer.class, propertyModel.getTypeData().getType());
        assertEquals(CUSTOM_SERIALIZATION, propertyModel.getPropertySerialization());
        assertEquals(FIELD_ACCESSOR, propertyModel.getPropertyAccessor());
        assertFalse(propertyModel.useDiscriminator());
    }

    private static final List<Annotation> ANNOTATIONS = Collections.singletonList(
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

    private static final PropertySerialization<Integer> CUSTOM_SERIALIZATION = value -> false;

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
