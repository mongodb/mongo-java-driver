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

import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;

public class BsonIgnoreDuplicatePropertyMultipleTypes {
    private final String stringField;
    private String altStringField;

    @BsonCreator
    public BsonIgnoreDuplicatePropertyMultipleTypes(@BsonProperty("stringField") final String stringField) {
        this.stringField = stringField;
    }

    public String getStringField() {
        return stringField;
    }

    @BsonIgnore
    public String getAltStringField() {
        return altStringField;
    }

    @BsonIgnore
    public void setAltStringField(String altStringField) {
        this.altStringField = altStringField;
    }

    @BsonIgnore
    public void setAltStringField(Integer i) {
        this.altStringField = i.toString();
    }

    @Override
    public String toString() {
        return "BsonIgnoreDuplicatePropertyMultipleTypes{"
                + "stringField='" + stringField + '\''
                + ", altStringField='" + altStringField + '\''
                + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BsonIgnoreDuplicatePropertyMultipleTypes that = (BsonIgnoreDuplicatePropertyMultipleTypes) o;

        if (stringField != null ? !stringField.equals(that.stringField) : that.stringField != null) {
            return false;
        }
        return altStringField != null ? altStringField.equals(that.altStringField) : that.altStringField == null;
    }

    @Override
    public int hashCode() {
        int result = stringField != null ? stringField.hashCode() : 0;
        result = 31 * result + (altStringField != null ? altStringField.hashCode() : 0);
        return result;
    }
}
