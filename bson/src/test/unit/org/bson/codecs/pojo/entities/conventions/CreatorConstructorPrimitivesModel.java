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

package org.bson.codecs.pojo.entities.conventions;

import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

public final class CreatorConstructorPrimitivesModel {
    private final int intField;
    private final String stringField;
    private final long longField;


    @BsonCreator
    public CreatorConstructorPrimitivesModel(@BsonProperty("intField") final int intField,
                                             @BsonProperty("stringField") final String stringField,
                                             @BsonProperty("longField") final long longField) {
        this.intField = intField;
        this.stringField = stringField;
        this.longField = longField;
    }

    /**
     * Returns the intField
     *
     * @return the intField
     */
    public int getIntField() {
        return intField;
    }

    /**
     * Returns the stringField
     *
     * @return the stringField
     */
    public String getStringField() {
        return stringField;
    }

    /**
     * Returns the longField
     *
     * @return the longField
     */
    public long getLongField() {
        return longField;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CreatorConstructorPrimitivesModel that = (CreatorConstructorPrimitivesModel) o;

        if (getIntField() != that.getIntField()) {
            return false;
        }
        if (getLongField() != that.getLongField()) {
            return false;
        }
        if (getStringField() != null ? !getStringField().equals(that.getStringField()) : that.getStringField() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getIntField();
        result = 31 * result + (getStringField() != null ? getStringField().hashCode() : 0);
        result = 31 * result + (int) (getLongField() ^ (getLongField() >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "CreatorConstructorPrimativesModel{"
                + "intField=" + intField
                + ", stringField='" + stringField + "'"
                + ", longField=" + longField
                + "}";
    }
}
