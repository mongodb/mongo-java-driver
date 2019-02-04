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

import com.mongodb.Function;
import com.mongodb.WriteConcern;
import org.bson.BsonDocument;

import static com.mongodb.operation.CommandOperationHelper.noOpRetryCommandModifier;

/**
 * An operation that aborts a transaction.
 *
 * @since 3.8
 */
@Deprecated
public class AbortTransactionOperation extends TransactionOperation {

    /**
     * Construct an instance.
     *
     * @param writeConcern the write concern
     */
    public AbortTransactionOperation(final WriteConcern writeConcern) {
        super(writeConcern);
    }

    @Override
    protected String getCommandName() {
        return "abortTransaction";
    }

    @Override
    protected Function<BsonDocument, BsonDocument> getRetryCommandModifier() {
        return noOpRetryCommandModifier();
    }
}
