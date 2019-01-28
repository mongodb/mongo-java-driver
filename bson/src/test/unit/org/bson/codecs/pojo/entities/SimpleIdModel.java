/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.bson.codecs.pojo.entities;

import org.bson.types.ObjectId;

public class SimpleIdModel {
    private ObjectId id;
    private Integer integerField;
    private String stringField;

    public SimpleIdModel(){
    }

    public SimpleIdModel(final Integer integerField, final String stringField) {
        this(null, integerField, stringField);
    }

    public SimpleIdModel(final ObjectId objectId, final Integer integerField, final String stringField) {
        this.id = objectId;
        this.integerField = integerField;
        this.stringField = stringField;
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(final ObjectId id) {
        this.id = id;
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

        SimpleIdModel that = (SimpleIdModel) o;

        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        if (integerField != null ? !integerField.equals(that.integerField) : that.integerField != null) {
            return false;
        }
        return stringField != null ? stringField.equals(that.stringField) : that.stringField == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (integerField != null ? integerField.hashCode() : 0);
        result = 31 * result + (stringField != null ? stringField.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SimpleIdModel{"
                + "id=" + id
                + ", integerField=" + integerField
                + ", stringField='" + stringField + '\''
                + '}';
    }
}
