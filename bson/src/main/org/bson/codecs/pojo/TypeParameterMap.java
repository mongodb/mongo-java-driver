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

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;


/**
 * Maps the index of a class's generic parameter type index to a field's.
 */
final class TypeParameterMap {
    private final Map<Integer, Integer> fieldToClassParamIndexMap;

    /**
     * Creates a new builder for the TypeParameterMap
     *
     * @return the builder
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a mapping of field type parameter index to the class type parameter index.
     *
     * <p>Note: A field index of -1, means the class's parameter type represents the whole field</p>
     *
     * @return a mapping of field type parameter index to the class type parameter index.
     */
    Map<Integer, Integer> getFieldToClassParamIndexMap() {
        return fieldToClassParamIndexMap;
    }

    boolean hasTypeParameters() {
        return !fieldToClassParamIndexMap.isEmpty();
    }

    /**
     * A builder for mapping field type parameter indices to the class type parameter indices
     */
    static final class Builder {
        private final Map<Integer, Integer> fieldToClassParamIndexMap = new HashMap<Integer, Integer>();

        private Builder() {
        }

        /**
         * Adds the type parameter index for a class that represents the whole field
         *
         * @param classTypeParameterIndex the class's type parameter index that represents the whole field
         * @return this
         */
        Builder addIndex(final int classTypeParameterIndex) {
            fieldToClassParamIndexMap.put(-1, classTypeParameterIndex);
            return this;
        }

        /**
         * Adds a mapping that represents the fields
         *
         * @param fieldTypeParameterIndex the field's type parameter index
         * @param classTypeParameterIndex the class's type parameter index
         * @return this
         */
        Builder addIndex(final int fieldTypeParameterIndex, final int classTypeParameterIndex) {
            fieldToClassParamIndexMap.put(fieldTypeParameterIndex, classTypeParameterIndex);
            return this;
        }

        /**
         * @return the TypeParameterMap
         */
        TypeParameterMap build() {
            if (fieldToClassParamIndexMap.size() > 1 && fieldToClassParamIndexMap.containsKey(-1)) {
                throw new IllegalStateException("You cannot have a generic field that also has type parameters.");
            }
            return new TypeParameterMap(fieldToClassParamIndexMap);
        }
    }

    @Override
    public String toString() {
        return "TypeParameterMap{"
                + "fieldToClassParamIndexMap=" + fieldToClassParamIndexMap
                + "}";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TypeParameterMap that = (TypeParameterMap) o;

        if (!getFieldToClassParamIndexMap().equals(that.getFieldToClassParamIndexMap())) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return getFieldToClassParamIndexMap().hashCode();
    }

    private TypeParameterMap(final Map<Integer, Integer> fieldToClassParamIndexMap) {
        this.fieldToClassParamIndexMap = unmodifiableMap(fieldToClassParamIndexMap);
    }
}
