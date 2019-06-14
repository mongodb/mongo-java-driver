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

package com.mongodb.client.model;

import org.bson.conversions.Bson;

import java.util.Collections;
import java.util.List;

/**
 * Options to control the behavior of the $merge aggregation stage
 *
 * @mongodb.driver.manual reference/operator/aggregation/merge/  $merge stage
 * @mongodb.server.release 4.2
 * @see Aggregates#merge(String, MergeOptions)
 * @see Aggregates#merge(com.mongodb.MongoNamespace, MergeOptions)
 * @since 3.11
 */
public final class MergeOptions {

    /**
     * The behavior of $merge if a result document and an existing document in the collection have the same value for the specified on
     * field(s).
     */
    public enum WhenMatched {
        /**
         *  Replace the existing document in the output collection with the matching results document.
         */
        REPLACE,

        /**
         *  Keep the existing document in the output collection.
         */
        KEEP_EXISTING,

        /**
         * Merge the matching documents
         */
        MERGE,

        /**
         * An aggregation pipeline to update the document in the collection.
         *
         * @see #whenMatchedPipeline(List)
         */
        PIPELINE,

        /**
         * Stop and fail the aggregation operation. Any changes to the output collection from previous documents are not reverted.
         */
        FAIL,
    }

    /**
     * The behavior of $merge if a result document does not match an existing document in the out collection.
     */
    public enum WhenNotMatched {
        /**
         * Insert the document into the output collection.
         */
        INSERT,

        /**
         * Discard the document; i.e. $merge does not insert the document into the output collection.
         */
        DISCARD,

        /**
         * Stop and fail the aggregation operation. Any changes to the output collection from previous documents are not reverted.
         */
        FAIL
    }

    private List<String> uniqueIdentifier;
    private WhenMatched whenMatched;
    private List<Variable<?>> variables;
    private List<Bson> whenMatchedPipeline;
    private WhenNotMatched whenNotMatched;

    /**
     * Gets the fields that act as a unique identifier for a document. The identifier determine if a results document matches an
     * already existing document in the output collection.
     *
     * @return the unique identifier
     */
    public List<String> getUniqueIdentifier() {
        return uniqueIdentifier;
    }

    /**
     * Sets the field that act as a unique identifier for a document. The identifier determine if a results document matches an
     * already existing document in the output collection.
     *
     * @param uniqueIdentifier the unique identifier
     * @return this
     */
    public MergeOptions uniqueIdentifier(final String uniqueIdentifier) {
        this.uniqueIdentifier = Collections.singletonList(uniqueIdentifier);
        return this;
    }

    /**
     * Sets the field that act as a unique identifier for a document. The identifier determine if a results document matches an
     * already existing document in the output collection.
     *
     * @param uniqueIdentifier the unique identifier
     * @return this
     */
    public MergeOptions uniqueIdentifier(final List<String> uniqueIdentifier) {
        this.uniqueIdentifier = uniqueIdentifier;
        return this;
    }

    /**
     * Gets the behavior of $merge if a result document and an existing document in the collection have the same value for the specified
     * on field(s).
     *
     * @return when matched
     */
    public WhenMatched getWhenMatched() {
        return whenMatched;
    }

    /**
     * Sets the behavior of $merge if a result document and an existing document in the collection have the same value for the specified
     * on field(s).
     *
     * @param whenMatched when matched
     * @return this
     */
    public MergeOptions whenMatched(final WhenMatched whenMatched) {
        this.whenMatched = whenMatched;
        return this;
    }

    /**
     * Gets the variables accessible for use in the whenMatched pipeline
     * @return the variables
     */
    public List<Variable<?>> getVariables() {
        return variables;
    }

    /**
     * Sets the variables accessible for use in the whenMatched pipeline.
     *
     * @param variables the variables
     * @return this
     */
    public MergeOptions variables(final List<Variable<?>> variables) {
        this.variables = variables;
        return this;
    }

    /**
     * Gets aggregation pipeline to update the document in the collection.
     *
     * @return when matched pipeline
     * @see WhenMatched#PIPELINE
     */
    public List<Bson> getWhenMatchedPipeline() {
        return whenMatchedPipeline;
    }

    /**
     * Sets aggregation pipeline to update the document in the collection.
     *
     * @param whenMatchedPipeline when matched pipeline
     * @return this
     * @see WhenMatched#PIPELINE
     */
    public MergeOptions whenMatchedPipeline(final List<Bson> whenMatchedPipeline) {
        this.whenMatchedPipeline = whenMatchedPipeline;
        return this;
    }

    /**
     * Gets the behavior of $merge if a result document does not match an existing document in the out collection.
     *
     * @return when not matched
     */
    public WhenNotMatched getWhenNotMatched() {
        return whenNotMatched;
    }

    /**
     * Sets the behavior of $merge if a result document does not match an existing document in the out collection.
     *
     * @param whenNotMatched when not matched
     * @return this
     */
    public MergeOptions whenNotMatched(final WhenNotMatched whenNotMatched) {
        this.whenNotMatched = whenNotMatched;
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MergeOptions that = (MergeOptions) o;

        if (uniqueIdentifier != null ? !uniqueIdentifier.equals(that.uniqueIdentifier) : that.uniqueIdentifier != null) {
            return false;
        }
        if (whenMatched != that.whenMatched) {
            return false;
        }
        if (variables != null ? !variables.equals(that.variables) : that.variables != null) {
            return false;
        }
        if (whenMatchedPipeline != null ? !whenMatchedPipeline.equals(that.whenMatchedPipeline) : that.whenMatchedPipeline != null) {
            return false;
        }
        if (whenNotMatched != that.whenNotMatched) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = uniqueIdentifier != null ? uniqueIdentifier.hashCode() : 0;
        result = 31 * result + (whenMatched != null ? whenMatched.hashCode() : 0);
        result = 31 * result + (variables != null ? variables.hashCode() : 0);
        result = 31 * result + (whenMatchedPipeline != null ? whenMatchedPipeline.hashCode() : 0);
        result = 31 * result + (whenNotMatched != null ? whenNotMatched.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MergeOptions{"
                + "uniqueIdentifier=" + uniqueIdentifier
                + ", whenMatched=" + whenMatched
                + ", variables=" + variables
                + ", whenMatchedPipeline=" + whenMatchedPipeline
                + ", whenNotMatched=" + whenNotMatched
                + '}';
    }
}
