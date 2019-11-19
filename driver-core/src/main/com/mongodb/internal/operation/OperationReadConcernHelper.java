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

import com.mongodb.internal.session.SessionContext;
import org.bson.BsonDocument;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ReadConcernHelper.getReadConcernDocument;

final class OperationReadConcernHelper {
    static void appendReadConcernToCommand(final SessionContext sessionContext, final BsonDocument commandDocument) {
        notNull("commandDocument", commandDocument);
        notNull("sessionContext", sessionContext);

        if (sessionContext.hasActiveTransaction()) {
            return;
        }

        BsonDocument readConcernDocument = getReadConcernDocument(sessionContext);
        if (!readConcernDocument.isEmpty()) {
            commandDocument.append("readConcern", readConcernDocument);
        }
    }

    private OperationReadConcernHelper() {
    }
}
