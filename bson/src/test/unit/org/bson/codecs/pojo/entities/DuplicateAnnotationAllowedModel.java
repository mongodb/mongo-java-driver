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

package org.bson.codecs.pojo.entities;

import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

public class DuplicateAnnotationAllowedModel {

    @Nullable
    private String id;

    @BsonIgnore
    private String ignoredString;

    @BsonProperty("property")
    private String propertyString;

    public DuplicateAnnotationAllowedModel() {
    }

    public DuplicateAnnotationAllowedModel(final String id) {
        this.id = id;
    }

    @Nullable
    public String getId() {
        return id;
    }

    public void setId(@Nullable final String id) {
        this.id = id;
    }

    @BsonIgnore
    public String getIgnoredString() {
        return ignoredString;
    }

    @BsonIgnore
    public void setIgnoredString(final String ignoredString) {
        this.ignoredString = ignoredString;
    }

    @BsonProperty("property")
    public String getPropertyString() {
        return propertyString;
    }

    @BsonProperty("property")
    public void setPropertyString(final String propertyString) {
        this.propertyString = propertyString;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DuplicateAnnotationAllowedModel that = (DuplicateAnnotationAllowedModel) o;

        return (Objects.equals(id, that.id));
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
