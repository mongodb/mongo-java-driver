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

public class PojoContainsRecord {
    private String stringField;
    private SimpleRecord simpleRecord;

    public PojoContainsRecord() {
    }

    public PojoContainsRecord(final String stringField, final SimpleRecord simpleRecord) {
        this.stringField = stringField;
        this.simpleRecord = simpleRecord;
    }

    public String getStringField() {
        return stringField;
    }

    public void setStringField(final String stringField) {
        this.stringField = stringField;
    }

    public Record getSimpleRecord() {
        return simpleRecord;
    }

    public void setSimpleRecord(final SimpleRecord simpleRecord) {
        this.simpleRecord = simpleRecord;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PojoContainsRecord that = (PojoContainsRecord) o;
        return Objects.equals(stringField, that.stringField) &&
                Objects.equals(simpleRecord, that.simpleRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stringField, simpleRecord);
    }

    @Override
    public String toString() {
        return "PojoContainsRecord{" +
                "name='" + stringField + '\'' +
                ", SimpleRecord=" + simpleRecord +
                '}';
    }
}
