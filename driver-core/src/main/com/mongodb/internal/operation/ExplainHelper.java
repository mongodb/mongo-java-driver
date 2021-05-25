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

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoInternalException;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;

final class ExplainHelper {

    // TODO REMOVE
    static BsonDocument asExplainCommand(final BsonDocument command, @Nullable final ExplainVerbosity explainVerbosity) {
        BsonDocument explainCommand = new BsonDocument("explain", command);
        if (explainVerbosity != null) {
            explainCommand.append("verbosity", getVerbosityAsString(explainVerbosity));
        }
        return explainCommand;
    }

    static CommandCreator asExplainCommandCreator(final CommandCreator commandCreator, @Nullable final ExplainVerbosity explainVerbosity) {
        return (clientSideOperationTimeout, serverDescription, connectionDescription) -> {
            BsonDocument explainCommand = new BsonDocument("explain",
                    commandCreator.create(clientSideOperationTimeout, serverDescription, connectionDescription));
            if (explainVerbosity != null) {
                explainCommand.append("verbosity", getVerbosityAsString(explainVerbosity));
            }
            return explainCommand;
        };
    }

    private static BsonString getVerbosityAsString(final ExplainVerbosity explainVerbosity) {
        switch (explainVerbosity) {
            case QUERY_PLANNER:
                return new BsonString("queryPlanner");
            case EXECUTION_STATS:
                return new BsonString("executionStats");
            case ALL_PLANS_EXECUTIONS:
                return new BsonString("allPlansExecution");
            default:
                throw new MongoInternalException(String.format("Unsupported explain verbosity %s", explainVerbosity));
        }
    }

    private ExplainHelper() {

    }
}
