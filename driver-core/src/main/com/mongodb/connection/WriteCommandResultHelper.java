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

package com.mongodb.connection;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoInternalException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.bulk.WriteRequest;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNumber;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb.bulk.WriteRequest.Type.UPDATE;
import static com.mongodb.internal.operation.WriteConcernHelper.createWriteConcernError;

final class WriteCommandResultHelper {

    static boolean hasError(final BsonDocument result) {
        return result.get("writeErrors") != null || result.get("writeConcernError") != null;
    }

    static BulkWriteResult getBulkWriteResult(final WriteRequest.Type type, final BsonDocument result) {
        int count = getCount(result);
        List<BulkWriteUpsert> upsertedItems = getUpsertedItems(result);
        return BulkWriteResult.acknowledged(type, count - upsertedItems.size(), getModifiedCount(type, result), upsertedItems);
    }

    static MongoBulkWriteException getBulkWriteException(final WriteRequest.Type type, final BsonDocument result,
                                                    final ServerAddress serverAddress) {
        if (!hasError(result)) {
            throw new MongoInternalException("This method should not have been called");
        }
        return new MongoBulkWriteException(getBulkWriteResult(type, result), getWriteErrors(result),
                                           getWriteConcernError(result), serverAddress);
    }

    @SuppressWarnings("unchecked")
    private static List<BulkWriteError> getWriteErrors(final BsonDocument result) {
        List<BulkWriteError> writeErrors = new ArrayList<BulkWriteError>();
        BsonArray writeErrorsDocuments = (BsonArray) result.get("writeErrors");
        if (writeErrorsDocuments != null) {
            for (BsonValue cur : writeErrorsDocuments) {
                BsonDocument curDocument = (BsonDocument) cur;
                writeErrors.add(new BulkWriteError(curDocument.getNumber("code").intValue(),
                                                   curDocument.getString("errmsg").getValue(),
                                                   curDocument.getDocument("errInfo", new BsonDocument()),
                                                   curDocument.getNumber("index").intValue()));
            }
        }
        return writeErrors;
    }

    private static WriteConcernError getWriteConcernError(final BsonDocument result) {
        BsonDocument writeConcernErrorDocument = (BsonDocument) result.get("writeConcernError");
        if (writeConcernErrorDocument == null) {
            return null;
        } else {
            return createWriteConcernError(writeConcernErrorDocument);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<BulkWriteUpsert> getUpsertedItems(final BsonDocument result) {
        BsonValue upsertedValue = result.get("upserted");
        if (upsertedValue == null) {
            return Collections.emptyList();
        } else {
            List<BulkWriteUpsert> bulkWriteUpsertList = new ArrayList<BulkWriteUpsert>();
            for (BsonValue upsertedItem : (BsonArray) upsertedValue) {
                BsonDocument upsertedItemDocument = (BsonDocument) upsertedItem;
                bulkWriteUpsertList.add(new BulkWriteUpsert(upsertedItemDocument.getNumber("index").intValue(),
                                                            upsertedItemDocument.get("_id")));
            }
            return bulkWriteUpsertList;
        }
    }

    private static int getCount(final BsonDocument result) {
        return result.getNumber("n").intValue();
    }

    private static Integer getModifiedCount(final WriteRequest.Type type, final BsonDocument result) {
        BsonNumber modifiedCount = result.getNumber("nModified", (type == UPDATE || type == REPLACE) ? null : new BsonInt32(0));
        return modifiedCount == null ? null : modifiedCount.intValue();

    }

    private WriteCommandResultHelper() {
    }
}
