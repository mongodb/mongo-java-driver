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

package com.mongodb.internal.connection;

import com.mongodb.ReadConcernLevel;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.notNull;

public final class ReadConcernHelper {

    public static BsonDocument getReadConcernDocument(final SessionContext sessionContext) {
        notNull("sessionContext", sessionContext);

        BsonDocument readConcernDocument = new BsonDocument();

        ReadConcernLevel level = sessionContext.getReadConcern().getLevel();
        if (level != null) {
            readConcernDocument.append("level", new BsonString(level.getValue()));
        }

        assertFalse(shouldAddAfterClusterTime(sessionContext) && shouldAddAtClusterTime(sessionContext));
        if (shouldAddAfterClusterTime(sessionContext)) {
            readConcernDocument.append("afterClusterTime", sessionContext.getOperationTime());
        } else if (shouldAddAtClusterTime(sessionContext)) {
            readConcernDocument.append("atClusterTime", sessionContext.getSnapshotTimestamp());
        }
        return readConcernDocument;
    }

    private static boolean shouldAddAtClusterTime(final SessionContext sessionContext) {
        return sessionContext.isSnapshot() && sessionContext.getSnapshotTimestamp() != null;
    }

    private static boolean shouldAddAfterClusterTime(final SessionContext sessionContext) {
        return sessionContext.isCausallyConsistent() && sessionContext.getOperationTime() != null;
    }

    private ReadConcernHelper() {
    }
}
