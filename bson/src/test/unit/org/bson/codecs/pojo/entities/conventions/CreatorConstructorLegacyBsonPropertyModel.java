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

import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.List;

public final class CreatorConstructorLegacyBsonPropertyModel {
    private final List<Integer> integersField;
    private String stringField;
    @BsonProperty("longField")
    private long myLongField;

    // Here we use the @BsonProperty using the actual field name, that has been set to read and write to "longField"
    @BsonCreator
    public CreatorConstructorLegacyBsonPropertyModel(@BsonProperty("integersField") final List<Integer> integerField,
                                                     @BsonProperty("myLongField") final long longField) {
        this.integersField = integerField;
        this.myLongField = longField;
    }

    public CreatorConstructorLegacyBsonPropertyModel(final List<Integer> integersField, final String stringField, final long longField) {
        this.integersField = integersField;
        this.stringField = stringField;
        this.myLongField = longField;
    }

    public List<Integer> getIntegersField() {
        return integersField;
    }

    public String getStringField() {
        return stringField;
    }

    public void setStringField(final String stringField) {
        this.stringField = stringField;
    }

    public long getMyLongField() {
        return myLongField;
    }

    public void setMyLongField(final long myLongField) {
        this.myLongField = myLongField;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CreatorConstructorLegacyBsonPropertyModel that = (CreatorConstructorLegacyBsonPropertyModel) o;

        if (getMyLongField() != that.getMyLongField()) {
            return false;
        }
        if (getIntegersField() != null ? !getIntegersField().equals(that.getIntegersField()) : that.getIntegersField() != null) {
            return false;
        }
        if (getStringField() != null ? !getStringField().equals(that.getStringField()) : that.getStringField() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getIntegersField() != null ? getIntegersField().hashCode() : 0;
        result = 31 * result + (getStringField() != null ? getStringField().hashCode() : 0);
        result = 31 * result + (int) (getMyLongField() ^ (getMyLongField() >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "CreatorConstructorModel{"
                + "integersField=" + integersField
                + ", stringField='" + stringField + "'"
                + ", myLongField=" + myLongField
                + "}";
    }
}
