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

import java.util.Arrays;

public class FieldAndPropertyTypeMismatchModel {
    private byte[] stringField;

    public FieldAndPropertyTypeMismatchModel() {
    }

    public FieldAndPropertyTypeMismatchModel(final String stringField) {
        this.stringField = stringField.getBytes();
    }

    public String getStringField() {
        return new String(stringField);
    }

    public void setStringField(final String stringField) {
        this.stringField = stringField.getBytes();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FieldAndPropertyTypeMismatchModel that = (FieldAndPropertyTypeMismatchModel) o;

        return Arrays.equals(stringField, that.stringField);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(stringField);
    }

    @Override
    public String toString() {
        return "FieldAndPropertyTypeMismatchModel{"
                + "stringField=" + new String(stringField)
                + '}';
    }
}
