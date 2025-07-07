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
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

/**
 * An operation that atomically finds and deletes a single document.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class FindAndDeleteOperation<T> extends BaseFindAndModifyOperation<T> {

    public FindAndDeleteOperation(final MongoNamespace namespace, final WriteConcern writeConcern, final boolean retryWrites,
            final Decoder<T> decoder) {
        super(namespace, writeConcern, retryWrites, decoder);
    }

    @Override
    public FindAndDeleteOperation<T> filter(@Nullable final BsonDocument filter) {
        super.filter(filter);
        return this;
    }

    @Override
    public FindAndDeleteOperation<T> projection(@Nullable final BsonDocument projection) {
        super.projection(projection);
        return this;
    }

    @Override
    public FindAndDeleteOperation<T> sort(@Nullable final BsonDocument sort) {
        super.sort(sort);
        return this;
    }

    @Override
    public FindAndDeleteOperation<T> hint(@Nullable final BsonDocument hint) {
        super.hint(hint);
        return this;
    }

    @Override
    public FindAndDeleteOperation<T> hintString(@Nullable final String hint) {
        super.hintString(hint);
        return this;
    }

    @Override
    public FindAndDeleteOperation<T> collation(@Nullable final Collation collation) {
        super.collation(collation);
        return this;
    }

    @Override
    public FindAndDeleteOperation<T> comment(@Nullable final BsonValue comment) {
        super.comment(comment);
        return this;
    }

    @Override
    public FindAndDeleteOperation<T> let(@Nullable final BsonDocument variables) {
        super.let(variables);
        return this;
    }

    protected FieldNameValidator getFieldNameValidator() {
        return NoOpFieldNameValidator.INSTANCE;
    }

    protected void specializeCommand(final BsonDocument commandDocument, final ConnectionDescription connectionDescription) {
        commandDocument.put("remove", BsonBoolean.TRUE);
    }
}
