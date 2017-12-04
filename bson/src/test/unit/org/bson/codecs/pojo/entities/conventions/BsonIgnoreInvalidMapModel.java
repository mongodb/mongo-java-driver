/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo.entities.conventions;

import org.bson.codecs.pojo.annotations.BsonIgnore;

import java.util.Map;

public class BsonIgnoreInvalidMapModel {

    private String stringField;

    @BsonIgnore
    private Map<Integer, Integer> invalidMap;

    public BsonIgnoreInvalidMapModel() {
    }

    public BsonIgnoreInvalidMapModel(final String stringField) {
        this.stringField = stringField;
    }

    public String getStringField() {
        return stringField;
    }

    public void setStringField(final String stringField) {
        this.stringField = stringField;
    }

    public Map<Integer, Integer> getInvalidMap() {
        return invalidMap;
    }

    public void setInvalidMap(final Map<Integer, Integer> invalidMap) {
        this.invalidMap = invalidMap;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BsonIgnoreInvalidMapModel that = (BsonIgnoreInvalidMapModel) o;

        if (stringField != null ? !stringField.equals(that.stringField) : that.stringField != null) {
            return false;
        }
        return invalidMap != null ? invalidMap.equals(that.invalidMap) : that.invalidMap == null;
    }

    @Override
    public int hashCode() {
        int result = stringField != null ? stringField.hashCode() : 0;
        result = 31 * result + (invalidMap != null ? invalidMap.hashCode() : 0);
        return result;
    }
}
