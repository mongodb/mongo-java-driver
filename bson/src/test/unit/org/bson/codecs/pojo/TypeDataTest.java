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

import org.bson.codecs.pojo.entities.GenericHolderModel;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("rawtypes")
public final class TypeDataTest {

    @Test
    public void testDefaults() {
        TypeData<String> typeData = TypeData.builder(String.class).build();

        assertEquals(String.class, typeData.getType());
        assertTrue(typeData.getTypeParameters().isEmpty());
    }

    @Test
    public void testListTypeParameters() {
        TypeData<String> subTypeData = TypeData.builder(String.class).build();
        TypeData<List> typeData = TypeData.builder(List.class).addTypeParameter(subTypeData).build();

        assertEquals(List.class, typeData.getType());
        assertEquals(singletonList(subTypeData), typeData.getTypeParameters());
    }

    @Test
    public void testMapTypeParameters() {
        TypeData<String> keyTypeData = TypeData.builder(String.class).build();
        TypeData<Integer> valueTypeData = TypeData.builder(Integer.class).build();
        TypeData<Map> typeData = TypeData.builder(Map.class).addTypeParameter(keyTypeData).addTypeParameter(valueTypeData).build();

        assertEquals(Map.class, typeData.getType());
        assertEquals(Arrays.<TypeData<?>>asList(keyTypeData, valueTypeData), typeData.getTypeParameters());
    }

    @Test
    public void testToString() {
        TypeData<String> stringType = TypeData.builder(String.class).build();
        TypeData<Map> mapTypeData = TypeData.builder(Map.class)
                .addTypeParameter(stringType)
                .addTypeParameter(TypeData.builder(Map.class).addTypeParameter(stringType).addTypeParameter(stringType).build())
                .build();

        assertEquals("TypeData{type=String}", stringType.toString());
        assertEquals("TypeData{type=Map, typeParameters=[String, Map<String, String>]}", mapTypeData.toString());
    }

    @Test
    public void testRecursiveTypeData() {
        TypeData<GenericHolderModel> typeData = TypeData.builder(GenericHolderModel.class)
                .addTypeParameter(TypeData.builder(GenericHolderModel.class)
                        .addTypeParameter(TypeData.builder(GenericHolderModel.class).build()).build()).build();

        typeData.toString();
    }
}
