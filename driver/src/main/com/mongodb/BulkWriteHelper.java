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

import com.mongodb.client.model.Collation;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;

import java.util.ArrayList;
import java.util.List;

final class BulkWriteHelper {

    static BulkWriteResult translateBulkWriteResult(final com.mongodb.bulk.BulkWriteResult bulkWriteResult,
                                                    final Decoder<DBObject> decoder) {
        if (bulkWriteResult.wasAcknowledged()) {
            Integer modifiedCount = (bulkWriteResult.isModifiedCountAvailable()) ? bulkWriteResult.getModifiedCount() : null;
            return new AcknowledgedBulkWriteResult(bulkWriteResult.getInsertedCount(), bulkWriteResult.getMatchedCount(),
                                                   bulkWriteResult.getDeletedCount(), modifiedCount,
                                                   translateBulkWriteUpserts(bulkWriteResult.getUpserts(), decoder));
        } else {
            return new UnacknowledgedBulkWriteResult();
        }
    }

    static List<BulkWriteUpsert> translateBulkWriteUpserts(final List<com.mongodb.bulk.BulkWriteUpsert> upserts,
                                                           final Decoder<DBObject> decoder) {
        List<BulkWriteUpsert> retVal = new ArrayList<BulkWriteUpsert>(upserts.size());
        for (com.mongodb.bulk.BulkWriteUpsert cur : upserts) {
            retVal.add(new com.mongodb.BulkWriteUpsert(cur.getIndex(), getUpsertedId(cur, decoder)));
        }
        return retVal;
    }

    private static Object getUpsertedId(final com.mongodb.bulk.BulkWriteUpsert cur, final Decoder<DBObject> decoder) {
        return decoder.decode(new BsonDocumentReader(new BsonDocument("_id", cur.getId())), DecoderContext.builder().build()).get("_id");
    }

    static BulkWriteException translateBulkWriteException(final MongoBulkWriteException e, final Decoder<DBObject> decoder) {
        return new BulkWriteException(translateBulkWriteResult(e.getWriteResult(), decoder), translateWriteErrors(e.getWriteErrors()),
                                           translateWriteConcernError(e.getWriteConcernError()), e.getServerAddress());
    }

    static WriteConcernError translateWriteConcernError(final com.mongodb.bulk.WriteConcernError writeConcernError) {
        return writeConcernError == null ? null : new WriteConcernError(writeConcernError.getCode(), writeConcernError.getMessage(),
                                                                        DBObjects.toDBObject(writeConcernError.getDetails()));
    }

    static List<BulkWriteError> translateWriteErrors(final List<com.mongodb.bulk.BulkWriteError> errors) {
        List<BulkWriteError> retVal = new ArrayList<BulkWriteError>(errors.size());
        for (com.mongodb.bulk.BulkWriteError cur : errors) {
            retVal.add(new BulkWriteError(cur.getCode(), cur.getMessage(), DBObjects.toDBObject(cur.getDetails()), cur.getIndex()));
        }
        return retVal;
    }

    static List<com.mongodb.bulk.WriteRequest> translateWriteRequestsToNew(final List<WriteRequest> writeRequests,
                                                                           final Collation collation) {
        List<com.mongodb.bulk.WriteRequest> retVal = new ArrayList<com.mongodb.bulk.WriteRequest>(writeRequests.size());
        for (WriteRequest cur : writeRequests) {
            com.mongodb.bulk.WriteRequest request = cur.toNew();
            if (request instanceof com.mongodb.bulk.UpdateRequest) {
                ((com.mongodb.bulk.UpdateRequest) request).collation(collation);
            } else if (request instanceof com.mongodb.bulk.DeleteRequest) {
                ((com.mongodb.bulk.DeleteRequest) request).collation(collation);
            }
            retVal.add(request);
        }
        return retVal;
    }

    private BulkWriteHelper() {
    }
}
