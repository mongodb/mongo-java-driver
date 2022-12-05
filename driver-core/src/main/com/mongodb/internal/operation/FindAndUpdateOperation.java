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
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class FindAndUpdateOperation<T> extends BaseFindAndModifyOperation<T> {
    private final BsonDocument update;
    private final List<BsonDocument> updatePipeline;
    private boolean returnOriginal = true;
    private boolean upsert;
    private Boolean bypassDocumentValidation;
    private List<BsonDocument> arrayFilters;

    public FindAndUpdateOperation(final MongoNamespace namespace, final WriteConcern writeConcern, final boolean retryWrites,
                                  final Decoder<T> decoder, final BsonDocument update) {
        super(namespace, writeConcern, retryWrites, decoder);
        this.update = notNull("update", update);
        this.updatePipeline = null;
    }

    public FindAndUpdateOperation(final MongoNamespace namespace, final WriteConcern writeConcern, final boolean retryWrites,
                                  final Decoder<T> decoder, final List<BsonDocument> update) {
        super(namespace, writeConcern, retryWrites, decoder);
        this.updatePipeline = update;
        this.update = null;
    }

    @Nullable
    public BsonDocument getUpdate() {
        return update;
    }

    @Nullable
    public List<BsonDocument> getUpdatePipeline() {
        return updatePipeline;
    }

    public boolean isReturnOriginal() {
        return returnOriginal;
    }

    public FindAndUpdateOperation<T> returnOriginal(final boolean returnOriginal) {
        this.returnOriginal = returnOriginal;
        return this;
    }

    public boolean isUpsert() {
        return upsert;
    }

    public FindAndUpdateOperation<T> upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public FindAndUpdateOperation<T> bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public FindAndUpdateOperation<T> arrayFilters(@Nullable final List<BsonDocument> arrayFilters) {
        this.arrayFilters = arrayFilters;
        return this;
    }

    public List<BsonDocument> getArrayFilters() {
        return arrayFilters;
    }

    @Override
    public FindAndUpdateOperation<T> filter(@Nullable final BsonDocument filter) {
        super.filter(filter);
        return this;
    }

    @Override
    public FindAndUpdateOperation<T> projection(@Nullable final BsonDocument projection) {
        super.projection(projection);
        return this;
    }

    @Override
    public FindAndUpdateOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        super.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public FindAndUpdateOperation<T> sort(@Nullable final BsonDocument sort) {
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
    public FindAndUpdateOperation<T> collation(@Nullable final Collation collation) {
        super.collation(collation);
        return this;
    }

    @Override
    public FindAndUpdateOperation<T> comment(@Nullable final BsonValue comment) {
        super.comment(comment);
        return this;
    }

    @Override
    public FindAndUpdateOperation<T> let(@Nullable final BsonDocument variables) {
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
