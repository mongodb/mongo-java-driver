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

package org.bson.codecs.pojo.entities.records;

import java.util.Objects;

public class PojoContainsGenericRecord {
    private String stringField;
    private GenericHolderRecord<String> genericRecord;

    public PojoContainsGenericRecord() {
    }

    public PojoContainsGenericRecord(final String stringField, final GenericHolderRecord<String> genericRecord) {
        this.stringField = stringField;
        this.genericRecord = genericRecord;
    }

    public String getStringField() {
        return stringField;
    }

    public void setStringField(final String stringField) {
        this.stringField = stringField;
    }

    public GenericHolderRecord<String> getGenericRecord() {
        return genericRecord;
    }

    public void setGenericRecord(final GenericHolderRecord<String> genericRecord) {
        this.genericRecord = genericRecord;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PojoContainsGenericRecord that = (PojoContainsGenericRecord) o;
        return Objects.equals(stringField, that.stringField) &&
                Objects.equals(genericRecord, that.genericRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stringField, genericRecord);
    }

    @Override
    public String toString() {
        return "PojoContainsGenericRecord{" +
                "stringField='" + stringField + '\'' +
                ", genericRecord=" + genericRecord +
                '}';
    }
}
