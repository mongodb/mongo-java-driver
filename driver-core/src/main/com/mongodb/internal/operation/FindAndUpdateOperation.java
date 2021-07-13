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

package com.mongodb.internal.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.validator.MappedFieldNameValidator;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.internal.validator.UpdateFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;
import org.bson.conversions.Bson;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfTrue;

/**
 * An operation that atomically finds and updates a single document.
 *
 * @param <T> the operations result type.
 * @since 3.0
 * @mongodb.driver.manual reference/command/findAndModify/ findAndModify
 */
public class FindAndUpdateOperation<T> extends BaseFindAndModifyOperation<T> {
    private final BsonDocument update;
    private final List<BsonDocument> updatePipeline;
    private boolean returnOriginal = true;
    private boolean upsert;
    private Boolean bypassDocumentValidation;
    private List<BsonDocument> arrayFilters;

    /**
     * Construct a new instance.
     *
     * @param namespace    the database and collection namespace for the operation.
     * @param writeConcern the writeConcern for the operation
     * @param retryWrites  if writes should be retried if they fail due to a network error.
     * @param decoder      the decoder for the result documents.
     * @param update       the document containing update operators.
     * @since 3.6
     */
    public FindAndUpdateOperation(final MongoNamespace namespace, final WriteConcern writeConcern, final boolean retryWrites,
                                  final Decoder<T> decoder, final BsonDocument update) {
        super(namespace, writeConcern, retryWrites, decoder);
        this.update = notNull("update", update);
        this.updatePipeline = null;
    }

    /**
     * Construct a new instance.
     *
     * @param namespace    the database and collection namespace for the operation.
     * @param writeConcern the writeConcern for the operation
     * @param retryWrites  if writes should be retried if they fail due to a network error.
     * @param decoder      the decoder for the result documents.
     * @param update       the pipeline containing update operators.
     * @since 3.11
     * @mongodb.server.release 4.2
     */
    public FindAndUpdateOperation(final MongoNamespace namespace, final WriteConcern writeConcern, final boolean retryWrites,
                                  final Decoder<T> decoder, final List<BsonDocument> update) {
        super(namespace, writeConcern, retryWrites, decoder);
        this.updatePipeline = update;
        this.update = null;
    }

    /**
     * Gets the document containing update operators
     *
     * @return the update document
     */
    @Nullable
    public BsonDocument getUpdate() {
        return update;
    }

    /**
     * Gets the pipeline containing update operators
     *
     * @return the update pipeline
     * @since 3.11
     * @mongodb.server.release 4.2
     */
    @Nullable
    public List<BsonDocument> getUpdatePipeline() {
        return updatePipeline;
    }

    /**
     * When false, returns the updated document rather than the original. The default is false.
     *
     * @return true if the original document should be returned
     */
    public boolean isReturnOriginal() {
        return returnOriginal;
    }

    /**
     * Set to false if the updated document rather than the original should be returned.
     *
     * @param returnOriginal set to false if the updated document rather than the original should be returned
     * @return this
     */
    public FindAndUpdateOperation<T> returnOriginal(final boolean returnOriginal) {
        this.returnOriginal = returnOriginal;
        return this;
    }

    /**
     * Returns true if a new document should be inserted if there are no matches to the query filter.  The default is false.
     *
     * @return true if a new document should be inserted if there are no matches to the query filter
     */
    public boolean isUpsert() {
        return upsert;
    }

    /**
     * Set to true if a new document should be inserted if there are no matches to the query filter.
     *
     * @param upsert true if a new document should be inserted if there are no matches to the query filter
     * @return this
     */
    public FindAndUpdateOperation<T> upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    /**
     * Gets the bypass document level validation flag
     *
     * @return the bypass document level validation flag
     * @since 3.2
     */
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * Sets the bypass document level validation flag.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @since 3.2
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     * @mongodb.server.release 3.2
     */
    public FindAndUpdateOperation<T> bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    /**
     * Sets the array filters option
     *
     * @param arrayFilters the array filters, which may be null
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public FindAndUpdateOperation<T> arrayFilters(final List<BsonDocument> arrayFilters) {
        this.arrayFilters = arrayFilters;
        return this;
    }

    /**
     * Returns the array filters option
     *
     * @return the array filters, which may be null
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public List<BsonDocument> getArrayFilters() {
        return arrayFilters;
    }

    @Override
    public FindAndUpdateOperation<T> filter(final BsonDocument filter) {
        super.filter(filter);
        return this;
    }

    @Override
    public FindAndUpdateOperation<T> projection(final BsonDocument projection) {
        super.projection(projection);
        return this;
    }

    @Override
    public FindAndUpdateOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        super.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public FindAndUpdateOperation<T> sort(final BsonDocument sort) {
        super.sort(sort);
        return this;
    }

    @Override
    public FindAndUpdateOperation<T> hint(@Nullable final Bson hint) {
        super.hint(hint);
        return this;
    }

    @Override
    public FindAndUpdateOperation<T> hintString(@Nullable final String hint) {
        super.hintString(hint);
        return this;
    }

    @Override
    public FindAndUpdateOperation<T> collation(final Collation collation) {
        super.collation(collation);
        return this;
    }

    @Override
    public FindAndUpdateOperation<T> comment(final BsonValue comment) {
        super.comment(comment);
        return this;
    }

    @Override
    public FindAndUpdateOperation<T> let(final BsonDocument variables) {
        super.let(variables);
        return this;
    }

    protected FieldNameValidator getFieldNameValidator() {
        Map<String, FieldNameValidator> map = new HashMap<>();
        map.put("update", new UpdateFieldNameValidator());
        return new MappedFieldNameValidator(new NoOpFieldNameValidator(), map);
    }

    protected void specializeCommand(final BsonDocument commandDocument, final ConnectionDescription connectionDescription) {
        commandDocument.put("new", new BsonBoolean(!isReturnOriginal()));
        putIfTrue(commandDocument, "upsert", isUpsert());

        if (getUpdatePipeline() != null) {
            commandDocument.put("update", new BsonArray(getUpdatePipeline()));
        } else {
            putIfNotNull(commandDocument, "update", getUpdate());
        }
        if (bypassDocumentValidation != null) {
            commandDocument.put("bypassDocumentValidation", BsonBoolean.valueOf(bypassDocumentValidation));
        }
        if (arrayFilters != null) {
            commandDocument.put("arrayFilters", new BsonArray(arrayFilters));
        }
    }
}
