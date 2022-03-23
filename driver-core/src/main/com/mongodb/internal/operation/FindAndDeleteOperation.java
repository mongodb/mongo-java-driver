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
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

/**
 * An operation that atomically finds and deletes a single document.
 *
 * @param <T> the operations result type.
 * @mongodb.driver.manual reference/command/findAndModify/ findAndModify
 * @since 3.0
 */
public class FindAndDeleteOperation<T> extends BaseFindAndModifyOperation<T> {

    /**
     * Construct a new instance.
     *
     * @param namespace    the database and collection namespace for the operation.
     * @param writeConcern the writeConcern for the operation
     * @param retryWrites  if writes should be retried if they fail due to a network error.
     * @param decoder      the decoder for the result documents.
     * @since 3.6
     */
    public FindAndDeleteOperation(final MongoNamespace namespace, final WriteConcern writeConcern, final boolean retryWrites,
                                  final Decoder<T> decoder) {
        super(namespace, writeConcern, retryWrites, decoder);
    }

    @Override
    public FindAndDeleteOperation<T> filter(final BsonDocument filter) {
        super.filter(filter);
        return this;
    }

    @Override
    public FindAndDeleteOperation<T> projection(final BsonDocument projection) {
        super.projection(projection);
        return this;
    }

    @Override
    public FindAndDeleteOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        super.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public FindAndDeleteOperation<T> sort(final BsonDocument sort) {
        super.sort(sort);
        return this;
    }

    @Override
    public FindAndDeleteOperation<T> hint(@Nullable final Bson hint) {
        super.hint(hint);
        return this;
    }

    @Override
    public FindAndDeleteOperation<T> hintString(@Nullable final String hint) {
        super.hintString(hint);
        return this;
    }

    @Override
    public FindAndDeleteOperation<T> collation(final Collation collation) {
        super.collation(collation);
        return this;
    }

    @Override
    public FindAndDeleteOperation<T> comment(final BsonValue comment) {
        super.comment(comment);
        return this;
    }

    protected FieldNameValidator getFieldNameValidator() {
        return new NoOpFieldNameValidator();
    }

    protected void specializeCommand(final BsonDocument commandDocument, final ConnectionDescription connectionDescription) {
        commandDocument.put("remove", BsonBoolean.TRUE);
    }

}
