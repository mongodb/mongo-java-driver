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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcernResult;
import com.mongodb.WriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.internal.bulk.WriteRequest;
import org.bson.BsonDocument;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class MongoBulkWriteExceptionHelper {

    public static MongoException translateSingleOperationBulkWriteResultException(
            final WriteRequest.Type type, final MongoBulkWriteException e) {
        MongoException exception;
        WriteConcernError writeConcernError = e.getWriteConcernError();
        if (writeConcernError != null) {
            exception = new MongoWriteConcernException(writeConcernError,
                    translateBulkWriteResult(type, e.getWriteResult()), e.getServerAddress(), e.getErrorLabels(), e);
        } else if (!e.getWriteErrors().isEmpty()) {
            exception = new MongoWriteException(new WriteError(e.getWriteErrors().get(0)), e.getServerAddress(),
                    e.getErrorLabels());
        } else {
            exception = new MongoWriteException(new WriteError(-1, "Unknown write error", new BsonDocument()),
                    e.getServerAddress(), e.getErrorLabels());
        }
        return exception;
    }

    private static WriteConcernResult translateBulkWriteResult(final WriteRequest.Type type, final BulkWriteResult writeResult) {
        switch (type) {
            case INSERT:
                return WriteConcernResult.acknowledged(writeResult.getInsertedCount(), false, null);
            case DELETE:
                return WriteConcernResult.acknowledged(writeResult.getDeletedCount(), false, null);
            case UPDATE:
            case REPLACE:
                return WriteConcernResult.acknowledged(writeResult.getMatchedCount() + writeResult.getUpserts().size(),
                        writeResult.getMatchedCount() > 0,
                        writeResult.getUpserts().isEmpty()
                                ? null : writeResult.getUpserts().get(0).getId());
            default:
                throw new MongoInternalException("Unhandled write request type: " + type);
        }
    }


    private MongoBulkWriteExceptionHelper() {
    }
}
