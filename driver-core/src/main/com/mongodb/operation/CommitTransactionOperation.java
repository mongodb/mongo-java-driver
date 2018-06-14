/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.operation;

import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;

import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static com.mongodb.operation.CommandOperationHelper.isRetryableException;

/**
 * An operation that commits a transaction.
 *
 * @since 3.8
 */
public class CommitTransactionOperation extends TransactionOperation {

    /**
     * Construct an instance.
     *
     * @param writeConcern the write concern
     */
    public CommitTransactionOperation(final WriteConcern writeConcern) {
        super(writeConcern);
    }

    @Override
    public Void execute(final WriteBinding binding) {
        try {
            return super.execute(binding);
        } catch (MongoException e) {
            addErrorLabels(e);
            throw e;
        }
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        super.executeAsync(binding, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                 if (t instanceof MongoException) {
                     addErrorLabels((MongoException) t);
                 }
                 callback.onResult(result, t);
            }
        });
    }

    private void addErrorLabels(final MongoException e) {
        if (isRetryableException(e) || e instanceof MongoTimeoutException) {
            e.addLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL);
        }
    }

    @Override
    protected String getCommandName() {
        return "commitTransaction";
    }
}
