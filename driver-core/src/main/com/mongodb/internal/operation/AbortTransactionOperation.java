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

import com.mongodb.Function;
import com.mongodb.WriteConcern;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import org.bson.BsonDocument;

import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.noOpRetryCommandModifier;

/**
 * An operation that aborts a transaction.
 *
 * @since 3.8
 */
public class AbortTransactionOperation extends TransactionOperation {
    private BsonDocument recoveryToken;

    /**
     * Construct an instance.
     *
     * @param writeConcern the write concern
     */
    public AbortTransactionOperation(final WriteConcern writeConcern) {
        super(writeConcern);
    }

    /**
     * Set the recovery token.
     *
     * @param recoveryToken the recovery token
     * @return the AbortTransactionOperation
     * @since 3.11
     */
    public AbortTransactionOperation recoveryToken(final BsonDocument recoveryToken) {
        this.recoveryToken = recoveryToken;
        return this;
    }

    @Override
    protected String getCommandName() {
        return "abortTransaction";
    }

    @Override
    CommandCreator getCommandCreator() {
        final CommandOperationHelper.CommandCreator creator = super.getCommandCreator();
        if (recoveryToken != null) {
            return new CommandCreator() {
                @Override
                public BsonDocument create(final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                    return creator.create(serverDescription, connectionDescription).append("recoveryToken", recoveryToken);
                }
            };
        }
        return creator;
    }

    @Override
    protected Function<BsonDocument, BsonDocument> getRetryCommandModifier() {
        return noOpRetryCommandModifier();
    }
}
