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

import com.mongodb.MongoWriteConcernException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcernResult;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

import java.util.stream.Collectors;

import static com.mongodb.internal.operation.AsyncOperationHelper.CommandWriteTransformerAsync;
import static com.mongodb.internal.operation.SyncOperationHelper.CommandWriteTransformer;
import static com.mongodb.internal.operation.WriteConcernHelper.createWriteConcernError;
import static com.mongodb.internal.operation.WriteConcernHelper.hasWriteConcernError;

final class FindAndModifyHelper {

    static <T> CommandWriteTransformer<BsonDocument, T> transformer() {
        return (result, connection) -> transformDocument(result, connection.getDescription().getServerAddress());
    }

    static <T> CommandWriteTransformerAsync<BsonDocument, T> asyncTransformer() {
        return (result, connection) -> transformDocument(result, connection.getDescription().getServerAddress());
    }

    @Nullable
    private static <T> T transformDocument(final BsonDocument result, final ServerAddress serverAddress) {
        if (hasWriteConcernError(result)) {
            throw new MongoWriteConcernException(
                    createWriteConcernError(result.getDocument("writeConcernError")),
                    createWriteConcernResult(result.getDocument("lastErrorObject", new BsonDocument())), serverAddress,
                    result.getArray("errorLabels", new BsonArray()).stream().map(i -> i.asString().getValue()).collect(Collectors.toSet()));
        }

        if (!result.isDocument("value")) {
            return null;
        }
        return BsonDocumentWrapperHelper.toDocument(result.getDocument("value", null));
    }

    private static WriteConcernResult createWriteConcernResult(final BsonDocument result) {
        BsonBoolean updatedExisting = result.getBoolean("updatedExisting", BsonBoolean.FALSE);

        return WriteConcernResult.acknowledged(result.getNumber("n", new BsonInt32(0)).intValue(),
                                               updatedExisting.getValue(), result.get("upserted"));
    }

    private FindAndModifyHelper() {
    }
}
