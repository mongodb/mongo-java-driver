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

import org.bson.codecs.Codec;
import org.bson.codecs.IntegerCodec;
import org.bson.codecs.pojo.entities.SimpleGenericsModel;
import org.bson.codecs.pojo.entities.SimpleModel;
import org.junit.Test;

import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public final class FieldModelTest {

    @Test
    public void testSimpleModelInteger() throws NoSuchFieldException {
        String fieldName = "integerField";
        FieldModel<Integer> fieldModel = FieldModel.<Integer>builder(SimpleModel.class.getDeclaredField(fieldName)).build();

        assertEquals(fieldName, fieldModel.getFieldName());
        assertEquals(fieldName, fieldModel.getDocumentFieldName());
        assertEquals(TypeData.builder(Integer.class).build(), fieldModel.getTypeData());
        assertNull(fieldModel.getCodec());
        assertTrue(fieldModel.shouldSerialize(1));
        assertNull(fieldModel.useDiscriminator());
        assertTrue(fieldModel.getFieldAccessor() instanceof FieldAccessorImpl);
    }

    @Test
    public void testSimpleModelGeneric() throws NoSuchFieldException {
        FieldModel<Object> fieldModel = FieldModel.<Object>builder(SimpleGenericsModel.class.getDeclaredField("myGenericField")).build();

        assertEquals(TypeData.builder(Object.class).build(), fieldModel.getTypeData());
        assertNull(fieldModel.getCodec());
        assertTrue(fieldModel.shouldSerialize(1));
        assertNull(fieldModel.useDiscriminator());
        assertTrue(fieldModel.getFieldAccessor() instanceof FieldAccessorImpl);
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testSimpleModelGenericList() throws NoSuchFieldException {
        FieldModel<?> fieldModel = FieldModel.builder(SimpleGenericsModel.class.getDeclaredField("myListField")).build();
        assertEquals(TypeData.builder(List.class)
                .addTypeParameter(TypeData.builder(Object.class).build()).build(), fieldModel.getTypeData());
    }

    @Test
    public void testConverter() throws NoSuchFieldException {
        String fieldName = "integerField";
        Codec<Integer> codec = new IntegerCodec();
        FieldModel<Integer> fieldModel = FieldModel.<Integer>builder(SimpleModel.class.getDeclaredField(fieldName))
                .codec(codec).build();
        assertEquals(codec, fieldModel.getCodec());
    }

}
