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

package com.mongodb.client.model.changestream;

import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.List;

/**
 * The update description for changed fields in a {@code $changeStream} operation.
 *
 * @since 3.6
 */
public final class UpdateDescription {
    private final List<String> removedFields;
    private final BsonDocument updatedFields;

    /**
     * Creates a new instance
     *
     * @param removedFields the list of fields that have been removed.
     * @param updatedFields the updated fields
     */
    @BsonCreator
    public UpdateDescription(@Nullable @BsonProperty("removedFields") final List<String> removedFields,
                             @Nullable @BsonProperty("updatedFields") final BsonDocument updatedFields) {
        this.removedFields = removedFields;
        this.updatedFields = updatedFields;
    }

    /**
     * Returns the removedFields
     *
     * @return the removedFields
     */
    @Nullable
    public List<String> getRemovedFields() {
        return removedFields;
    }

    /**
     * Returns the updatedFields
     *
     * @return the updatedFields
     */
    @Nullable
    public BsonDocument getUpdatedFields() {
        return updatedFields;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UpdateDescription that = (UpdateDescription) o;

        if (removedFields != null ? !removedFields.equals(that.getRemovedFields()) : that.getRemovedFields() != null) {
            return false;
        }
        if (updatedFields != null ? !updatedFields.equals(that.getUpdatedFields()) : that.getUpdatedFields() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = removedFields != null ? removedFields.hashCode() : 0;
        result = 31 * result + (updatedFields != null ? updatedFields.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "UpdateDescription{"
                + "removedFields=" + removedFields
                + ", updatedFields=" + updatedFields
                + "}";
    }
}
