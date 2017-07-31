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

package org.bson.codecs.pojo.entities;

public final class ConstructorNotPublicModel {
    private final Integer integerField;

    ConstructorNotPublicModel(final Integer integerField) {
        this.integerField = integerField;
    }

    public static ConstructorNotPublicModel create(final Integer integerField) {
        return new ConstructorNotPublicModel(integerField);
    }

    public Integer getIntegerField() {
        return integerField;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConstructorNotPublicModel that = (ConstructorNotPublicModel) o;

        if (getIntegerField() != null ? !getIntegerField().equals(that.getIntegerField()) : that.getIntegerField() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getIntegerField() != null ? getIntegerField().hashCode() : 0;
        return result;
    }

    @Override
    public String toString() {
        return "ConstructorNotPublicModel{"
                + "integerField=" + integerField
                + "}";
    }
}
