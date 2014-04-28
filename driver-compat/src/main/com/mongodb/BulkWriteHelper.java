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

package com.mongodb;

import java.util.ArrayList;
import java.util.List;

final class BulkWriteHelper {

    static BulkWriteResult translateBulkWriteResult(final org.mongodb.BulkWriteResult bulkWriteResult) {
        if (bulkWriteResult.isAcknowledged()) {
            Integer modifiedCount = (bulkWriteResult.isModifiedCountAvailable()) ? bulkWriteResult.getModifiedCount() : null;
            return new AcknowledgedBulkWriteResult(bulkWriteResult.getInsertedCount(), bulkWriteResult.getUpdatedCount(),
                                                   bulkWriteResult.getRemovedCount(), modifiedCount,
                                                   translateBulkWriteUpserts(bulkWriteResult.getUpserts()));
        } else {
            return new UnacknowledgedBulkWriteResult();
        }
    }

    static List<BulkWriteUpsert> translateBulkWriteUpserts(final List<org.mongodb.BulkWriteUpsert> upserts) {
        List<BulkWriteUpsert> retVal = new ArrayList<BulkWriteUpsert>(upserts.size());
        for (org.mongodb.BulkWriteUpsert cur : upserts) {
            retVal.add(new com.mongodb.BulkWriteUpsert(cur.getIndex(), cur.getId()));
        }
        return retVal;
    }

    static BulkWriteException translateBulkWriteException(final org.mongodb.BulkWriteException e) {
        return new BulkWriteException(translateBulkWriteResult(e.getWriteResult()), translateWriteErrors(e.getWriteErrors()),
                                      translateWriteConcernError(e.getWriteConcernError()), new ServerAddress(e.getServerAddress()));
    }

    static WriteConcernError translateWriteConcernError(final org.mongodb.WriteConcernError writeConcernError) {
        return writeConcernError == null ? null : new WriteConcernError(writeConcernError.getCode(), writeConcernError.getMessage(),
                                                                        DBObjects.toDBObject(writeConcernError.getDetails()));
    }

    static List<BulkWriteError> translateWriteErrors(final List<org.mongodb.BulkWriteError> errors) {
        List<BulkWriteError> retVal = new ArrayList<BulkWriteError>(errors.size());
        for (org.mongodb.BulkWriteError cur : errors) {
            retVal.add(new BulkWriteError(cur.getCode(), cur.getMessage(), DBObjects.toDBObject(cur.getDetails()), cur.getIndex()));
        }
        return retVal;
    }

    @SuppressWarnings("unchecked")
    static List<WriteRequest> translateWriteRequests(final List<org.mongodb.operation.WriteRequest> writeRequests) {
        List<WriteRequest> retVal = new ArrayList<WriteRequest>(writeRequests.size());
        for (org.mongodb.operation.WriteRequest cur : writeRequests) {
            switch (cur.getType()) {
                case INSERT:
                    retVal.add(new InsertRequest((org.mongodb.operation.InsertRequest<DBObject>) cur));
                    break;
                case UPDATE:
                    retVal.add(new UpdateRequest((org.mongodb.operation.UpdateRequest) cur));
                    break;
                case REPLACE:
                    retVal.add(new ReplaceRequest((org.mongodb.operation.ReplaceRequest<DBObject>) cur));
                    break;
                case REMOVE:
                    retVal.add(new RemoveRequest((org.mongodb.operation.RemoveRequest) cur));
                    break;
                default:
                    throw new MongoInternalException("Unexpected write type: " + cur.getType());
            }
        }
        return retVal;

    }

    static List<org.mongodb.operation.WriteRequest> translateWriteRequestsToNew(final List<WriteRequest> writeRequests) {
        List<org.mongodb.operation.WriteRequest> retVal = new ArrayList<org.mongodb.operation.WriteRequest>(writeRequests.size());
        for (WriteRequest cur : writeRequests) {
            retVal.add(cur.toNew());
        }
        return retVal;
    }

    private BulkWriteHelper() {
    }
}
