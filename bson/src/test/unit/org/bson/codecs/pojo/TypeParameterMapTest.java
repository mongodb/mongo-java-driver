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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class TypeParameterMapTest {

    @Test
    public void testDefault() {
        TypeParameterMap typeParameterMap = TypeParameterMap.builder().build();
        assertTrue(typeParameterMap.getPropertyToClassParamIndexMap().isEmpty());
    }

    @Test
    public void testClassParamMapsToField() {
        TypeParameterMap typeParameterMap = TypeParameterMap.builder().addIndex(1).build();
        Map<Integer, Either<Integer, TypeParameterMap>> expected = new HashMap<>();
        expected.put(-1, Either.left(1));
        assertEquals(expected, typeParameterMap.getPropertyToClassParamIndexMap());
    }

    @Test
    public void testMapsClassAndFieldIndices() {
        TypeParameterMap typeParameterMap = TypeParameterMap.builder().addIndex(1, 2).addIndex(2, 2).build();
        Map<Integer, Either<Integer, TypeParameterMap>> expected = new HashMap<>();
        expected.put(1, Either.left(2));
        expected.put(2, Either.left(2));
        assertEquals(expected, typeParameterMap.getPropertyToClassParamIndexMap());
    }

    @Test
    public void testFieldCannotBeGenericAndContainTypeParameters() {
        assertThrows(IllegalStateException.class, () ->
                TypeParameterMap.builder().addIndex(1).addIndex(2, 2).build());
    }
}
