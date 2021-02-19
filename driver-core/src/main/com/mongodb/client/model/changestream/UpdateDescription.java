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

import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

/**
 * The update description for changed fields in a {@code $changeStream} operation.
 *
 * @since 3.6
 */
public final class UpdateDescription {
    private final List<String> removedFields;
    private final BsonDocument updatedFields;
    private final List<TruncatedArray> truncatedArrays;

    /**
     * Creates a new instance
     *
     * @param removedFields See {@link #UpdateDescription(List, BsonDocument, List)}.
     * @param updatedFields See {@link #UpdateDescription(List, BsonDocument, List)}.
     * @see #UpdateDescription(List, BsonDocument, List)
     */
    public UpdateDescription(@Nullable final List<String> removedFields,
                             @Nullable final BsonDocument updatedFields) {
        this(removedFields, updatedFields, null);
    }

    /**
     * @param removedFields   Names of the fields that were removed.
     * @param updatedFields   Information about the updated fields.
     * @param truncatedArrays Information about the updated fields of the {@linkplain org.bson.BsonType#ARRAY array} type
     *                        when the changes are reported as truncations.
     *                        If {@code null}, then {@link #getTruncatedArrays()} returns an {@linkplain List#isEmpty() empty} {@link List}.
     */
    @BsonCreator
    public UpdateDescription(
            @Nullable @BsonProperty("removedFields") final List<String> removedFields,
            @Nullable @BsonProperty("updatedFields") final BsonDocument updatedFields,
            @Nullable @BsonProperty("truncatedArrays") final List<TruncatedArray> truncatedArrays) {
        this.removedFields = removedFields;
        this.updatedFields = updatedFields;
        this.truncatedArrays = truncatedArrays == null ? emptyList() : truncatedArrays;
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
     * Returns information about the updated fields excluding the fields reported via {@link #getTruncatedArrays()}.
     * <p>
     * Despite {@linkplain org.bson.BsonType#ARRAY array} fields reported via {@link #getTruncatedArrays()} being excluded from the
     * information returned by this method, changes to fields of the elements of the array values may be reported via this method.
     * For example, given the original field {@code "arrayField": ["foo", {"a": "bar"}, 1, 2, 3]}
     * and the updated field {@code "arrayField": ["foo", {"a": "bar", "b": 3}]}, the following is how such a change may be reported:
     * <table>
     *   <tr>
     *     <th>Method</th>
     *     <th>Result</th>
     *   </tr>
     *   <tr>
     *     <td>{@link #getUpdatedFields()}</td>
     *     <td>{"arrayField.1.b": 3}</td>
     *   </tr>
     *   <tr>
     *     <td>{@link #getTruncatedArrays()}</td>
     *     <td>{"field": "arrayField", "newSize": 2}</td>
     *   </tr>
     * </table>
     *
     * @return {@code updatedFields}.
     * @see #getTruncatedArrays()
     */
    @Nullable
    public BsonDocument getUpdatedFields() {
        return updatedFields;
    }

    /**
     * Returns information about the updated fields of the {@linkplain org.bson.BsonType#ARRAY array} type
     * when the changes are reported as truncations.
     *
     * @return {@code truncatedArrays}.
     * There are no guarantees on the mutability of the {@code List} returned.
     * @see #getUpdatedFields()
     */
    @NonNull
    public List<TruncatedArray> getTruncatedArrays() {
        return truncatedArrays;
    }

    /**
     * @return {@code true} if and only if all of the following is true for the compared objects
     * <ul>
     *     <li>{@linkplain #getClass()} results are the same</li>
     *     <li>{@linkplain #getRemovedFields()} results are {@linkplain Objects#equals(Object, Object) equal}</li>
     *     <li>{@linkplain #getUpdatedFields()} results are {@linkplain Objects#equals(Object, Object) equal}</li>
     *     <li>
     *         {@linkplain #getTruncatedArrays()} results are {@linkplain Objects#equals(Object, Object) equal}
     *         or both contain no data ({@code null} or {@linkplain List#isEmpty() empty}).
     *     </li>
     * </ul>
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UpdateDescription that = (UpdateDescription) o;
        return Objects.equals(removedFields, that.removedFields)
                && Objects.equals(updatedFields, that.updatedFields)
                && Objects.equals(truncatedArrays, that.truncatedArrays);
    }

    @Override
    public int hashCode() {
        return Objects.hash(removedFields, updatedFields, truncatedArrays);
    }

    @Override
    public String toString() {
        return "UpdateDescription{"
                + "removedFields=" + removedFields
                + ", updatedFields=" + updatedFields
                + ", truncatedArrays=" + truncatedArrays
                + "}";
    }
}
