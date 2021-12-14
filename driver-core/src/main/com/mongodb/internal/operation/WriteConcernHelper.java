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
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.connection.ProtocolHelper;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.internal.operation.CommandOperationHelper.addRetryableWriteErrorLabel;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotFour;

/**
 * This class is NOT part of the public API. It may change at any time without notification.
 */
public final class WriteConcernHelper {

    public static void appendWriteConcernToCommand(final WriteConcern writeConcern, final BsonDocument commandDocument,
                                                   final ConnectionDescription description) {
        if (writeConcern != null && !writeConcern.isServerDefault() && serverIsAtLeastVersionThreeDotFour(description)) {
            commandDocument.put("writeConcern", writeConcern.asDocument());
        }
    }

    public static void throwOnWriteConcernError(final BsonDocument result, final ServerAddress serverAddress, final int maxWireVersion) {
        if (hasWriteConcernError(result)) {
            MongoException exception = ProtocolHelper.createSpecialException(result, serverAddress, "errmsg");
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
                WriteConcernResult.acknowledged(0, false, null),
                serverAddress,
                result.getArray("errorLabels", new BsonArray()));
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
