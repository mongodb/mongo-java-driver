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
    private final BsonDocument disambiguatedPaths;

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
     * @since 4.3
     */
    public UpdateDescription(
            @Nullable @BsonProperty("removedFields") final List<String> removedFields,
            @Nullable @BsonProperty("updatedFields") final BsonDocument updatedFields,
            @Nullable @BsonProperty("truncatedArrays") final List<TruncatedArray> truncatedArrays) {
        this(removedFields, updatedFields, truncatedArrays, null);
    }

    /**
     * @param removedFields   Names of the fields that were removed.
     * @param updatedFields   Information about the updated fields.
     * @param truncatedArrays Information about the updated fields of the {@linkplain org.bson.BsonType#ARRAY array} type
     *                        when the changes are reported as truncations. If {@code null}, then {@link #getTruncatedArrays()} returns
     *                        an {@linkplain List#isEmpty() empty} {@link List}.
     * @param disambiguatedPaths a document containing a map that associates an update path to an array containing the path components
     *                           used in the update document.
     * @since 4.8
     */
    @BsonCreator
    public UpdateDescription(
            @Nullable @BsonProperty("removedFields") final List<String> removedFields,
            @Nullable @BsonProperty("updatedFields") final BsonDocument updatedFields,
            @Nullable @BsonProperty("truncatedArrays") final List<TruncatedArray> truncatedArrays,
            @Nullable @BsonProperty("disambiguatedPaths") final BsonDocument disambiguatedPaths) {
        this.removedFields = removedFields;
        this.updatedFields = updatedFields;
        this.truncatedArrays = truncatedArrays == null ? emptyList() : truncatedArrays;
        this.disambiguatedPaths = disambiguatedPaths;
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
     *   <caption>An example showing how the aforementioned change may be reported</caption>
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
     * @since 4.3
     */
    @NonNull
    public List<TruncatedArray> getTruncatedArrays() {
        return truncatedArrays;
    }

    /**
     * A document containing a map that associates an update path to an array containing the path components used in the update document.
     *
     * <p>
     * This data can be used in combination with the other fields in an `UpdateDescription` to determine the actual path in the document
     * that was updated. This is necessary in cases where a key contains dot-separated strings (i.e., <code>{"a.b": "c"}</code>) or a
     * document contains a numeric literal string key (i.e., <code>{ "a": { "0": "a" } }</code>. Note that in this
     * scenario, the numeric key can't be the top level key, because <code>{ "0": "a" }</code> is not ambiguous - update paths
     * would simply be <code>'0'</code> which is unambiguous because BSON documents cannot have arrays at the top level.).
     * </p>
     * <p>
     * Each entry in the document maps an update path to an array which contains the actual path used when the document was updated. For
     * example, given a document with the following shape <code>{ "a": { "0": 0 } }</code> and an update of
     * <code>{ $inc: { "a.0": 1 } }</code>, <code>disambiguatedPaths</code> would look like the following:
     * <code> { "a.0": ["a", "0"] }</code>.
     * </p>
     * <p>
     * In each array, all elements will be returned as strings, except for array indices, which will be returned as 32-bit integers.
     * </p>
     *
     * @return the disambiguated paths as a BSON document, which may be null
     * @since 4.8
     * @mongodb.server.release 6.1
     */
    @Nullable
    public BsonDocument getDisambiguatedPaths() {
        return disambiguatedPaths;
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
                && Objects.equals(truncatedArrays, that.truncatedArrays)
                && Objects.equals(disambiguatedPaths, that.disambiguatedPaths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(removedFields, updatedFields, truncatedArrays, disambiguatedPaths);
    }

    @Override
    public String toString() {
        return "UpdateDescription{"
                + "removedFields=" + removedFields
                + ", updatedFields=" + updatedFields
                + ", truncatedArrays=" + truncatedArrays
                + ", disambiguatedPaths=" + disambiguatedPaths
                + "}";
    }
}
