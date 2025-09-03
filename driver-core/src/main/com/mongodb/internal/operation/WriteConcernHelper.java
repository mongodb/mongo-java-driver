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

package com.mongodb.internal.operation;

import com.mongodb.MongoException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.connection.ProtocolHelper;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.mongodb.internal.operation.CommandOperationHelper.addRetryableWriteErrorLabel;

/**
 * This class is NOT part of the public API. It may change at any time without notification.
 */
public final class WriteConcernHelper {

    public static void appendWriteConcernToCommand(final WriteConcern writeConcern, final BsonDocument commandDocument) {
        if (writeConcern != null && !writeConcern.isServerDefault()) {
            commandDocument.put("writeConcern", writeConcern.asDocument());
        }
    }
    @Nullable
    public static WriteConcern cloneWithoutTimeout(@Nullable final WriteConcern writeConcern) {
        if (writeConcern == null || writeConcern.getWTimeout(TimeUnit.MILLISECONDS) == null) {
            return writeConcern;
        }

        WriteConcern mapped;
        Object w = writeConcern.getWObject();
        if (w == null) {
            mapped = WriteConcern.ACKNOWLEDGED;
        } else {
            mapped = w instanceof Integer ? new WriteConcern((Integer) w) : new WriteConcern((String) w);
        }
        return mapped.withJournal(writeConcern.getJournal());
    }

    public static void throwOnWriteConcernError(final BsonDocument result, final ServerAddress serverAddress,
                                                final int maxWireVersion, final TimeoutContext timeoutContext) {
        if (hasWriteConcernError(result)) {
            MongoException exception = ProtocolHelper.createSpecialException(result, serverAddress, "errmsg", timeoutContext);
            if (exception == null) {
                exception = createWriteConcernException(result, serverAddress);
            }
            addRetryableWriteErrorLabel(exception, maxWireVersion);
            throw exception;
        }
    }

    public static boolean hasWriteConcernError(final BsonDocument result) {
        return result.containsKey("writeConcernError");
    }

    public static MongoWriteConcernException createWriteConcernException(final BsonDocument result, final ServerAddress serverAddress) {
        return new MongoWriteConcernException(
                createWriteConcernError(result.getDocument("writeConcernError")),
                WriteConcernResult.acknowledged(0, false, null), serverAddress,
                result.getArray("errorLabels", new BsonArray()).stream().map(i -> i.asString().getValue())
                        .collect(Collectors.toSet()));
    }

    public static WriteConcernError createWriteConcernError(final BsonDocument writeConcernErrorDocument) {
        return new WriteConcernError(writeConcernErrorDocument.getNumber("code").intValue(),
                writeConcernErrorDocument.getString("codeName", new BsonString("")).getValue(),
                writeConcernErrorDocument.getString("errmsg").getValue(),
                writeConcernErrorDocument.getDocument("errInfo", new BsonDocument()));
    }

    private WriteConcernHelper() {
    }
}
