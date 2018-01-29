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

package com.mongodb.operation;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.session.SessionContext;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import static com.mongodb.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.operation.CommandOperationHelper.executeRetryableCommand;

abstract class BaseFindAndModifyOperation<T> implements AsyncWriteOperation<T>, WriteOperation<T> {

    @Override
    public T execute(final WriteBinding binding) {
        return executeRetryableCommand(binding, getDatabaseName(), null, getFieldNameValidator(),
                CommandResultDocumentCodec.create(getDecoder(), "value"),
                getCommandCreator(binding.getSessionContext()),
                FindAndModifyHelper.<T>transformer());
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<T> callback) {
        executeRetryableCommand(binding, getDatabaseName(), null, getFieldNameValidator(),
                CommandResultDocumentCodec.create(getDecoder(), "value"),
                getCommandCreator(binding.getSessionContext()),
                FindAndModifyHelper.<T>transformer(), callback);
    }

    protected abstract String getDatabaseName();

    protected abstract Decoder<T> getDecoder();

    protected abstract CommandCreator getCommandCreator(SessionContext sessionContext);

    protected abstract FieldNameValidator getFieldNameValidator();
}
