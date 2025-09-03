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

package org.bson.codecs.pojo.entities.conventions;

import org.bson.codecs.pojo.annotations.BsonExtraElements;

import java.util.Map;
import java.util.Objects;

public class BsonExtraElementsMapModel {

    private Integer integerField;
    private String stringField;
    @BsonExtraElements
    private Map<String, String> extraElements;

    public BsonExtraElementsMapModel(){
    }

    public BsonExtraElementsMapModel(final Integer integerField, final String stringField, final Map<String, String> extraElements) {
        this.integerField = integerField;
        this.stringField = stringField;
        this.extraElements = extraElements;
    }

    public Integer getIntegerField() {
        return integerField;
    }

    public BsonExtraElementsMapModel setIntegerField(final Integer integerField) {
        this.integerField = integerField;
        return this;
    }

    public String getStringField() {
        return stringField;
    }

    public BsonExtraElementsMapModel setStringField(final String stringField) {
        this.stringField = stringField;
        return this;
    }

    public Map<String, String> getExtraElements() {
        return extraElements;
    }

    public BsonExtraElementsMapModel setExtraElement(final String key, final String value) {
        extraElements.put(key, value);
        return this;
    }

    public Object get(final String key) {
        return extraElements.get(key);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BsonExtraElementsMapModel that = (BsonExtraElementsMapModel) o;
        return Objects.equals(integerField, that.integerField)
                && Objects.equals(stringField, that.stringField)
                && Objects.equals(extraElements, that.extraElements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(integerField, stringField, extraElements);
    }

    @Override
    public String toString() {
        return "BsonExtraElementsModel{"
                + "integerField=" + integerField
                + ", stringField='" + stringField + '\''
                + ", extraElements=" + extraElements
                + '}';
    }
}
