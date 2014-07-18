/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.protocol;

import com.mongodb.MongoInternalException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNumber;
import org.bson.BsonValue;
import org.mongodb.BulkWriteError;
import org.mongodb.BulkWriteException;
import org.mongodb.BulkWriteResult;
import org.mongodb.BulkWriteUpsert;
import org.mongodb.CommandResult;
import org.mongodb.WriteConcernError;
import org.mongodb.operation.WriteRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mongodb.operation.WriteRequest.Type.REPLACE;
import static org.mongodb.operation.WriteRequest.Type.UPDATE;

final class WriteCommandResultHelper {

    static boolean hasError(final CommandResult commandResult) {
        return commandResult.getResponse().get("writeErrors") != null || commandResult.getResponse().get("writeConcernError") != null;
    }

    static BulkWriteResult getBulkWriteResult(final WriteRequest.Type type, final CommandResult commandResult) {
        int count = getCount(commandResult);
        List<BulkWriteUpsert> upsertedItems = getUpsertedItems(commandResult);
        return new AcknowledgedBulkWriteResult(type, count - upsertedItems.size(), getModifiedCount(type, commandResult), upsertedItems);
    }

    static BulkWriteException getBulkWriteException(final WriteRequest.Type type, final CommandResult commandResult) {
        if (!hasError(commandResult)) {
            throw new MongoInternalException("This method should not have been called");
        }
        return new BulkWriteException(getBulkWriteResult(type, commandResult), getWriteErrors(commandResult),
                                      getWriteConcernError(commandResult), commandResult.getAddress());
    }

    @SuppressWarnings("unchecked")
    private static List<BulkWriteError> getWriteErrors(final CommandResult commandResult) {
        List<BulkWriteError> writeErrors = new ArrayList<BulkWriteError>();
        BsonArray writeErrorsDocuments = (BsonArray) commandResult.getResponse().get("writeErrors");
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

    private static WriteConcernError getWriteConcernError(final CommandResult commandResult) {
        BsonDocument writeConcernErrorDocument = (BsonDocument) commandResult.getResponse().get("writeConcernError");
        if (writeConcernErrorDocument == null) {
            return null;
        } else {
            return new WriteConcernError(writeConcernErrorDocument.getNumber("code").intValue(),
                                         writeConcernErrorDocument.getString("errmsg").getValue(),
                                         writeConcernErrorDocument.getDocument("errInfo", new BsonDocument()));
        }
    }

    @SuppressWarnings("unchecked")
    private static List<BulkWriteUpsert> getUpsertedItems(final CommandResult commandResult) {
        BsonValue upsertedValue = commandResult.getResponse().get("upserted");
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

    private static int getCount(final CommandResult commandResult) {
        return commandResult.getResponse().getNumber("n").intValue();
    }

    private static Integer getModifiedCount(final WriteRequest.Type type, final CommandResult commandResult) {
        BsonNumber modifiedCount = commandResult.getResponse().getNumber("nModified",
                                                     (type == UPDATE || type == REPLACE) ? null : new BsonInt32(0));
        return modifiedCount == null ? null : modifiedCount.intValue();

    }

    private WriteCommandResultHelper() {
    }
}
