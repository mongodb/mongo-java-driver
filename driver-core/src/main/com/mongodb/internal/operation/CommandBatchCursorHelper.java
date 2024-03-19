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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoQueryException;
import com.mongodb.ServerCursor;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.FieldNameValidator;

import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionFourDotFour;
import static java.lang.String.format;
import static java.util.Collections.singletonList;

final class CommandBatchCursorHelper {

    static final String FIRST_BATCH = "firstBatch";
    static final String NEXT_BATCH = "nextBatch";
    static final FieldNameValidator NO_OP_FIELD_NAME_VALIDATOR = new NoOpFieldNameValidator();
    static final String MESSAGE_IF_CLOSED_AS_CURSOR = "Cursor has been closed";
    static final String MESSAGE_IF_CLOSED_AS_ITERATOR = "Iterator has been closed";

    static final String MESSAGE_IF_CONCURRENT_OPERATION = "Another operation is currently in progress, concurrent operations are not "
            + "supported";

    static BsonDocument getMoreCommandDocument(
            final long cursorId, final ConnectionDescription connectionDescription, final MongoNamespace namespace, final int batchSize,
            final long maxTimeMS, @Nullable final BsonValue comment) {
        BsonDocument document = new BsonDocument("getMore", new BsonInt64(cursorId))
                .append("collection", new BsonString(namespace.getCollectionName()));

        if (batchSize != 0) {
            document.append("batchSize", new BsonInt32(batchSize));
        }
        if (maxTimeMS != 0) {
            document.append("maxTimeMS", new BsonInt64(maxTimeMS));
        }
        if (serverIsAtLeastVersionFourDotFour(connectionDescription)) {
            putIfNotNull(document, "comment", comment);
        }
        return document;
    }

    static <T> CommandCursorResult<T> logCommandCursorResult(final CommandCursorResult<T> commandCursorResult) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Received batch of %d documents with cursorId %d from server %s", commandCursorResult.getResults().size(),
                    commandCursorResult.getCursorId(), commandCursorResult.getServerAddress()));
        }
        return commandCursorResult;
    }

    static BsonDocument getKillCursorsCommand(final MongoNamespace namespace, final ServerCursor serverCursor,
            final OperationContext operationContext) {
        BsonDocument command = new BsonDocument("killCursors", new BsonString(namespace.getCollectionName()))
                .append("cursors", new BsonArray(singletonList(new BsonInt64(serverCursor.getId()))));
        putIfNotZero(command, "maxTimeMS", operationContext.getTimeoutContext().getMaxTimeMS());
        return command;
    }


    static MongoQueryException translateCommandException(final MongoCommandException commandException, final ServerCursor cursor) {
        if (commandException.getErrorCode() == 43) {
            return new MongoCursorNotFoundException(cursor.getId(), commandException.getResponse(), cursor.getAddress());
        } else {
            return new MongoQueryException(commandException.getResponse(), commandException.getServerAddress());
        }
    }

    private CommandBatchCursorHelper() {
    }
}
