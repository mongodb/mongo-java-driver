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

import org.bson.codecs.pojo.entities.GenericHolderModel;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

        assertEquals(ArrayList.class, typeData.getType());
        assertEquals(singletonList(subTypeData), typeData.getTypeParameters());
    }

    @Test
    public void testMapTypeParameters() {
        TypeData<String> keyTypeData = TypeData.builder(String.class).build();
        TypeData<Integer> valueTypeData = TypeData.builder(Integer.class).build();
        TypeData<Map> typeData = TypeData.builder(Map.class).addTypeParameter(keyTypeData).addTypeParameter(valueTypeData).build();

        assertEquals(HashMap.class, typeData.getType());
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
        assertEquals("TypeData{type=HashMap, typeParameters=[String, HashMap<String, String>]}", mapTypeData.toString());
    }

    @Test
    public void testRecursiveTypeData() {
        TypeData<GenericHolderModel> typeData = TypeData.builder(GenericHolderModel.class)
                .addTypeParameter(TypeData.builder(GenericHolderModel.class)
                        .addTypeParameter(TypeData.builder(GenericHolderModel.class).build()).build()).build();

        typeData.toString();
    }

    @Test(expected = IllegalStateException.class)
    public void testListNoParamsValidation() {
        TypeData.builder(List.class).build();
    }

    @Test(expected = IllegalStateException.class)
    public void testListToManyParamsValidation() {
        TypeData<String> stringTypeData = TypeData.builder(String.class).build();
        TypeData.builder(List.class).addTypeParameter(stringTypeData).addTypeParameter(stringTypeData).build();
    }

    @Test(expected = IllegalStateException.class)
    public void testMapNoParamsValidation() {
        TypeData.builder(Map.class).build();
    }

    @Test(expected = IllegalStateException.class)
    public void testMapKeyValidation() {
        TypeData.builder(Map.class).addTypeParameter(TypeData.builder(Integer.class).build()).build();
    }

    @Test(expected = IllegalStateException.class)
    public void testInvalidMapImplementationValidation() {
        TypeData.builder(InvalidMapImplementation.class).build();
    }

    @Test(expected = IllegalStateException.class)
    public void testMapToManyParamsValidation() {
        TypeData<String> stringTypeData = TypeData.builder(String.class).build();
        TypeData.builder(Map.class).addTypeParameter(stringTypeData).addTypeParameter(stringTypeData)
                .addTypeParameter(stringTypeData).build();
    }
}
