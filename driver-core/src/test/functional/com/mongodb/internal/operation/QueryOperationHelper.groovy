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

package com.mongodb.internal.operation

import org.bson.BsonDocument

class QueryOperationHelper {
    static BsonDocument sanitizeExplainResult(BsonDocument document) {
        document.remove('ok')
        document.remove('millis')
        document.remove('executionStats')
        document.remove('serverInfo')
        document.remove('executionTimeMillis')
        document.remove('operationTime')
        document.remove('$clusterTime')
        document
    }

    static BsonDocument getKeyPattern(BsonDocument explainPlan) {
        BsonDocument winningPlan = explainPlan.getDocument('queryPlanner').getDocument('winningPlan')
        if (winningPlan.containsKey('queryPlan')) {
            BsonDocument queryPlan = winningPlan.getDocument('queryPlan')
            if (queryPlan.containsKey('inputStage')) {
                return queryPlan.getDocument('inputStage').getDocument('keyPattern')
            }
        } else if (winningPlan.containsKey('inputStage')) {
            return winningPlan.getDocument('inputStage').getDocument('keyPattern')
        } else if (winningPlan.containsKey('shards')) {
            return winningPlan.getArray('shards')[0].asDocument().getDocument('winningPlan')
                    .getDocument('inputStage').getDocument('keyPattern')
        }
    }
}
