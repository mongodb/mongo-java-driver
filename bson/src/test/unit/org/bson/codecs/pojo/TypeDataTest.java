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
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    public void newInstanceResolvesTypeVariableAgainstContext() throws NoSuchFieldException {
        Field listField = Holder.class.getDeclaredField("list");
        Type listGenericType = listField.getGenericType();
        List<TypeVariable<?>> typeParams = asList(Holder.class.getTypeParameters());

        TypeData<String> resolvedString = TypeData.builder(String.class).build();
        TypeData<Holder> currentResolved = TypeData.builder(Holder.class).addTypeParameter(resolvedString).build();

        TypeData<List> expected = TypeData.builder(List.class).addTypeParameter(resolvedString).build();
        TypeData<List> actual = TypeData.newInstance(listGenericType, List.class, typeParams, currentResolved);
        assertEquals(expected, actual);
    }

    @Test
    public void newInstanceResolvesTypeVariableMiss() throws NoSuchFieldException {
        Field listField = Holder.class.getDeclaredField("list");
        Type listGenericType = listField.getGenericType();

        List<TypeVariable<?>> emptyTypeParams = emptyList();
        TypeData<String> resolvedString = TypeData.builder(String.class).build();
        TypeData<Holder> currentResolved = TypeData.builder(Holder.class).addTypeParameter(resolvedString).build();

        TypeData<List> expected = TypeData.builder(List.class).addTypeParameter(TypeData.builder(Object.class).build()).build();
        TypeData<List> actual = TypeData.newInstance(listGenericType, List.class, emptyTypeParams, currentResolved);
        assertEquals(expected, actual);
    }

    @Test
    public void newInstanceTypeVariableHitWithNullCurrentResolved() throws NoSuchFieldException {
        Field listField = Holder.class.getDeclaredField("list");
        Type listGenericType = listField.getGenericType();

        List<TypeVariable<?>> typeParams = asList(Holder.class.getTypeParameters());

        TypeData<List> expected = TypeData.builder(List.class).addTypeParameter(TypeData.builder(Object.class).build()).build();
        TypeData<List> actual = TypeData.newInstance(listGenericType, List.class, typeParams, null);
        assertEquals(expected, actual);
    }

    @Test
    public void newInstanceTypeVariableHitIndexOutOfRange() throws NoSuchFieldException {
        Field listField = Holder.class.getDeclaredField("list");
        Type listGenericType = listField.getGenericType();

        List<TypeVariable<?>> typeParams = asList(Holder.class.getTypeParameters());
        TypeData<Holder> currentResolved = TypeData.builder(Holder.class).build();

        TypeData<List> expected = TypeData.builder(List.class).addTypeParameter(TypeData.builder(Object.class).build()).build();
        TypeData<List> actual = TypeData.newInstance(listGenericType, List.class, typeParams, currentResolved);
        assertEquals(expected, actual);
    }

    @Test
    public void newInstanceResolvesNestedParameterizedTypeContainingTypeVariable() throws NoSuchFieldException {
        Field nestedField = Holder.class.getDeclaredField("nestedMap");
        Type nestedGenericType = nestedField.getGenericType();

        List<TypeVariable<?>> typeParams = asList(Holder.class.getTypeParameters());
        TypeData<Integer> resolvedInt = TypeData.builder(Integer.class).build();
        TypeData<Holder> currentResolved = TypeData.builder(Holder.class).addTypeParameter(resolvedInt).build();

        TypeData<String> stringType = TypeData.builder(String.class).build();
        TypeData<Map> expected = TypeData.builder(Map.class).addTypeParameter(stringType).addTypeParameter(resolvedInt).build();
        TypeData<Map> actual = TypeData.newInstance(nestedGenericType, Map.class, typeParams, currentResolved);
        assertEquals(expected, actual);
    }

    @Test
    public void newInstanceResolvesConcreteClassArgument() throws NoSuchFieldException {
        Field concreteField = Holder.class.getDeclaredField("concrete");
        Type concreteGenericType = concreteField.getGenericType();

        List<TypeVariable<?>> typeParams = asList(Holder.class.getTypeParameters());
        TypeData<Holder> currentResolved = TypeData.builder(Holder.class)
                .addTypeParameter(TypeData.builder(Integer.class).build()).build();

        TypeData<List> expected = TypeData.builder(List.class).addTypeParameter(TypeData.builder(String.class).build()).build();
        TypeData<List> actual = TypeData.newInstance(concreteGenericType, List.class, typeParams, currentResolved);
        assertEquals(expected, actual);
    }

    @Test
    public void newInstanceWithEmptyTypeParamsReturnsRawTypeData() throws NoSuchFieldException {
        Field stringField = NonGeneric.class.getDeclaredField("s");
        Type stringGenericType = stringField.getGenericType();

        TypeData<String> actual = TypeData.newInstance(
                stringGenericType,
                String.class,
                emptyList(),
                null
        );

        TypeData<String> expected = TypeData.builder(String.class).build();
        assertEquals(expected, actual);
    }

    @Test
    public void newInstanceErasesGenericArrayTypeToObjectArray() throws NoSuchFieldException {
        Field arrayField = Holder.class.getDeclaredField("genericArray");
        Type arrayGenericType = arrayField.getGenericType();

        List<TypeVariable<?>> typeParams = asList(Holder.class.getTypeParameters());
        TypeData<Holder> currentResolved = TypeData.builder(Holder.class)
                .addTypeParameter(TypeData.builder(String.class).build()).build();

        TypeData<Object[]> actual = TypeData.newInstance(arrayGenericType, Object[].class, typeParams, currentResolved);
        TypeData<Object[]> expected = TypeData.builder(Object[].class).build();
        assertEquals(expected, actual);
    }

    @Test
    public void newInstanceErasesGenericArrayTypeArgumentToObject() throws NoSuchFieldException {
        // List<T[]> — the List is a ParameterizedType whose type argument T[] is a GenericArrayType.
        // resolveTypeArgument is called on T[] and must hit the else branch, erasing it to Object.
        Field field = Holder.class.getDeclaredField("listOfGenericArrays");
        Type genericType = field.getGenericType();

        List<TypeVariable<?>> typeParams = asList(Holder.class.getTypeParameters());
        TypeData<Holder> currentResolved = TypeData.builder(Holder.class)
                .addTypeParameter(TypeData.builder(String.class).build()).build();

        TypeData<List> expected = TypeData.builder(List.class)
                .addTypeParameter(TypeData.builder(Object.class).build()).build();
        TypeData<List> actual = TypeData.newInstance(genericType, List.class, typeParams, currentResolved);
        assertEquals(expected, actual);
    }

    @Test
    public void newInstanceResolvesBoundedWildcardToUpperBound() throws NoSuchFieldException {
        Field boundedWildcardField = Holder.class.getDeclaredField("boundedWildcard");
        Type boundedWildcardGenericType = boundedWildcardField.getGenericType();

        List<TypeVariable<?>> typeParams = asList(Holder.class.getTypeParameters());
        TypeData<Holder> currentResolved = TypeData.builder(Holder.class)
                .addTypeParameter(TypeData.builder(String.class).build()).build();

        TypeData<List> expected = TypeData.builder(List.class)
                .addTypeParameter(TypeData.builder(Number.class).build()).build();
        TypeData<List> actual = TypeData.newInstance(boundedWildcardGenericType, List.class, typeParams, currentResolved);
        assertEquals(expected, actual);
    }

    private static class Holder<T> {
        private List<T> list;
        private List<String> concrete;
        private Map<String, T> nestedMap;
        private List<? extends Number> boundedWildcard;
        private T[] genericArray;
        private List<T[]> listOfGenericArrays;
    }

    private static class NonGeneric {
        private String s;
    }
}
