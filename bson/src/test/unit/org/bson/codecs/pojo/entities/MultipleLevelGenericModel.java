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

public final class MultipleLevelGenericModel<A> {

    private A stringField;
    private GenericTreeModel<A, Integer> nested;

    public MultipleLevelGenericModel() {
    }

    public MultipleLevelGenericModel(final A stringField, final GenericTreeModel<A, Integer> nested) {
        this.stringField = stringField;
        this.nested = nested;
    }

    public A getStringField() {
        return stringField;
    }

    public void setStringField(final A stringField) {
        this.stringField = stringField;
    }

    public GenericTreeModel<A, Integer> getNested() {
        return nested;
    }

    public void setNested(final GenericTreeModel<A, Integer> nested) {
        this.nested = nested;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MultipleLevelGenericModel<?> that = (MultipleLevelGenericModel<?>) o;

        if (getStringField() != null ? !getStringField().equals(that.getStringField()) : that.getStringField() != null) {
            return false;
        }
        if (getNested() != null ? !getNested().equals(that.getNested()) : that.getNested() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getStringField() != null ? getStringField().hashCode() : 0;
        result = 31 * result + (getNested() != null ? getNested().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MultipleLevelGenericModel{"
                + "stringField=" + stringField
                + ", nested=" + nested
                + "}";
    }
}
