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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class PojoSpecializationHelper {

    @SuppressWarnings("unchecked")
    static <V> TypeData<V> specializeTypeData(final TypeData<V> typeData, final List<TypeData<?>> typeParameters,
                                              final TypeParameterMap typeParameterMap) {
        if (!typeParameterMap.hasTypeParameters() || typeParameters.isEmpty()) {
            return typeData;
        }

        Map<Integer, Either<Integer, TypeParameterMap>> propertyToClassParamIndexMap = typeParameterMap.getPropertyToClassParamIndexMap();
        Either<Integer, TypeParameterMap> classTypeParamRepresentsWholeField = propertyToClassParamIndexMap.get(-1);
        if (classTypeParamRepresentsWholeField != null) {
            Integer index = classTypeParamRepresentsWholeField.map(i -> i, e -> {
                throw new IllegalStateException("Invalid state, the whole class cannot be represented by a subtype.");
            });
            return (TypeData<V>) typeParameters.get(index);
        } else {
            return getTypeData(typeData, typeParameters, propertyToClassParamIndexMap);
        }
    }

    private static <V> TypeData<V> getTypeData(final TypeData<V> typeData, final List<TypeData<?>> specializedTypeParameters,
                                               final Map<Integer, Either<Integer, TypeParameterMap>> propertyToClassParamIndexMap) {
        List<TypeData<?>> subTypeParameters = new ArrayList<>(typeData.getTypeParameters());
        for (int i = 0; i < typeData.getTypeParameters().size(); i++) {
            subTypeParameters.set(i, getTypeData(subTypeParameters.get(i), specializedTypeParameters, propertyToClassParamIndexMap, i));
        }
        return TypeData.builder(typeData.getType()).addTypeParameters(subTypeParameters).build();
    }

    private static TypeData<?> getTypeData(final TypeData<?> typeData, final List<TypeData<?>> specializedTypeParameters,
                                           final Map<Integer, Either<Integer, TypeParameterMap>> propertyToClassParamIndexMap,
                                           final int index) {
        if (!propertyToClassParamIndexMap.containsKey(index)) {
            return typeData;
        }
        return propertyToClassParamIndexMap.get(index).map(l -> {
                    if (typeData.getTypeParameters().isEmpty()) {
                        // Represents the whole typeData
                        return specializedTypeParameters.get(l);
                    } else {
                        // Represents a single nested type parameter within this typeData
                        TypeData.Builder<?> builder = TypeData.builder(typeData.getType());
                        List<TypeData<?>> typeParameters = new ArrayList<>(typeData.getTypeParameters());
                        typeParameters.set(index, specializedTypeParameters.get(l));
                        builder.addTypeParameters(typeParameters);
                        return builder.build();
                    }
                },
                r -> {
                    // Represents a child type parameter of this typeData
                    return getTypeData(typeData, specializedTypeParameters, r.getPropertyToClassParamIndexMap());
                });
    }

    private PojoSpecializationHelper() {
    }
}
