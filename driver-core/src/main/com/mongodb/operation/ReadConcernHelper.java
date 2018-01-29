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

import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.assertions.Assertions.notNull;

final class ReadConcernHelper {

    static void appendReadConcernToCommand(final ReadConcern readConcern, final SessionContext sessionContext,
                                           final BsonDocument commandDocument) {
        notNull("readConcern", readConcern);
        notNull("sessionContext", sessionContext);
        notNull("commandDocument", commandDocument);
        BsonDocument readConcernDocument = new BsonDocument();
        ReadConcernLevel level = getReadConcernLevel(readConcern, sessionContext);
        if (level != null) {
            readConcernDocument.append("level", new BsonString(level.getValue()));
        }
        if (shouldAddAfterClusterTime(sessionContext)) {
            readConcernDocument.append("afterClusterTime", sessionContext.getOperationTime());
        }
        if (!readConcernDocument.isEmpty()) {
            commandDocument.append("readConcern", readConcernDocument);
        }
    }

    private static ReadConcernLevel getReadConcernLevel(final ReadConcern readConcern, final SessionContext sessionContext) {
        if (readConcern.getLevel() == null && shouldAddAfterClusterTime(sessionContext)) {
            return ReadConcernLevel.LOCAL;
        } else {
            return readConcern.getLevel();
        }
    }

    private static boolean shouldAddAfterClusterTime(final SessionContext sessionContext) {
        return sessionContext.isCausallyConsistent() && sessionContext.getOperationTime() != null;
    }

    private ReadConcernHelper() {
    }
}
