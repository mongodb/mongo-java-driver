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

package org.bson.codecs.pojo.entities;

import java.util.Objects;

public final class SimpleWithStaticModel {
    private Integer integerField;
    private String stringField;

    public SimpleWithStaticModel(){
    }

    public SimpleWithStaticModel(final Integer integerField, final String stringField) {
        this.integerField = integerField;
        this.stringField = stringField;
    }

    public Integer getIntegerField() {
        return integerField;
    }

    public void setIntegerField(final Integer integerField) {
        this.integerField = integerField;
    }

    public String getStringField() {
        return stringField;
    }

    public static void getStringField$Annotations() {
        // Mimics the static kotlin synthetic annotations field
    }

    public static void setIntegerField$Annotations() {
        // Mimics the static kotlin synthetic annotations field
    }

    public void getStringField$Alternative() {
        // Non static void getter field
    }

    public void setStringField(final String stringField) {
        this.stringField = stringField;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleWithStaticModel that = (SimpleWithStaticModel) o;
        return Objects.equals(integerField, that.integerField) && Objects.equals(stringField, that.stringField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(integerField, stringField);
    }

    @Override
    public String toString() {
        return "SimpleWithStaticModel{"
                + "integerField=" + integerField
                + ", stringField='" + stringField + "'"
                + "}";
    }
}
