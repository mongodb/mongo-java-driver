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
import com.mongodb.internal.validator.ReplacingDocumentFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfTrue;
import static java.util.Collections.singletonMap;

/**
 * An operation that atomically finds and replaces a single document.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class FindAndReplaceOperation<T> extends BaseFindAndModifyOperation<T> {
    private final BsonDocument replacement;
    private boolean returnOriginal = true;
    private boolean upsert;
    private Boolean bypassDocumentValidation;

    public FindAndReplaceOperation(final MongoNamespace namespace, final WriteConcern writeConcern, final boolean retryWrites,
            final Decoder<T> decoder, final BsonDocument replacement) {
        super(namespace, writeConcern, retryWrites, decoder);
        this.replacement = notNull("replacement", replacement);
    }

    public BsonDocument getReplacement() {
        return replacement;
    }

    public boolean isReturnOriginal() {
        return returnOriginal;
    }

    public FindAndReplaceOperation<T> returnOriginal(final boolean returnOriginal) {
        this.returnOriginal = returnOriginal;
        return this;
    }

    public boolean isUpsert() {
        return upsert;
    }

    public FindAndReplaceOperation<T> upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public FindAndReplaceOperation<T> bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public FindAndReplaceOperation<T> filter(@Nullable final BsonDocument filter) {
        super.filter(filter);
        return this;
    }

    @Override
    public FindAndReplaceOperation<T> projection(@Nullable final BsonDocument projection) {
        super.projection(projection);
        return this;
    }

    @Override
    public FindAndReplaceOperation<T> sort(@Nullable final BsonDocument sort) {
        super.sort(sort);
        return this;
    }

    @Override
    public FindAndReplaceOperation<T> hint(@Nullable final BsonDocument hint) {
        super.hint(hint);
        return this;
    }

    @Override
    public FindAndReplaceOperation<T> hintString(@Nullable final String hint) {
        super.hintString(hint);
        return this;
    }

    @Override
    public FindAndReplaceOperation<T> collation(@Nullable final Collation collation) {
        super.collation(collation);
        return this;
    }

    @Override
    public FindAndReplaceOperation<T> comment(@Nullable final BsonValue comment) {
        super.comment(comment);
        return this;
    }

    @Override
    public FindAndReplaceOperation<T> let(@Nullable final BsonDocument variables) {
        super.let(variables);
        return this;
    }

    @Override
    protected FieldNameValidator getFieldNameValidator() {
        return new MappedFieldNameValidator(
                NoOpFieldNameValidator.INSTANCE,
                singletonMap("update", ReplacingDocumentFieldNameValidator.INSTANCE));
    }

    @Override
    protected void specializeCommand(final BsonDocument commandDocument, final ConnectionDescription connectionDescription) {
        commandDocument.put("new", new BsonBoolean(!isReturnOriginal()));
        putIfTrue(commandDocument, "upsert", isUpsert());
        commandDocument.put("update", getReplacement());
        if (bypassDocumentValidation != null) {
            commandDocument.put("bypassDocumentValidation", BsonBoolean.valueOf(bypassDocumentValidation));
        }
    }
}
