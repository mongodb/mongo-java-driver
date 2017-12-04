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

import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

public class CustomPropertyCodecOptionalModel {
    private final Optional<String> optionalField;

    @BsonCreator
    public CustomPropertyCodecOptionalModel(final @BsonProperty("optionalField") Optional<String> optionalField) {
        this.optionalField = optionalField == null ? Optional.<String>empty() : optionalField;
    }

    public Optional<String> getOptionalField() {
        return optionalField;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CustomPropertyCodecOptionalModel that = (CustomPropertyCodecOptionalModel) o;

        return optionalField != null ? optionalField.equals(that.optionalField) : that.optionalField == null;
    }

    @Override
    public int hashCode() {
        return optionalField != null ? optionalField.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "CustomPropertyCodecOptionalModel{"
                + "optionalField=" + optionalField
                + '}';
    }
}
