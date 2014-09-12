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

import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;

import java.util.ArrayList;
import java.util.List;

final class BulkWriteHelper {

    static BulkWriteResult translateBulkWriteResult(final org.mongodb.BulkWriteResult bulkWriteResult, final Decoder<DBObject> decoder) {
        if (bulkWriteResult.isAcknowledged()) {
            Integer modifiedCount = (bulkWriteResult.isModifiedCountAvailable()) ? bulkWriteResult.getModifiedCount() : null;
            return new AcknowledgedBulkWriteResult(bulkWriteResult.getInsertedCount(), bulkWriteResult.getMatchedCount(),
                                                   bulkWriteResult.getRemovedCount(), modifiedCount,
                                                   translateBulkWriteUpserts(bulkWriteResult.getUpserts(), decoder));
        } else {
            return new UnacknowledgedBulkWriteResult();
        }
    }

    static List<BulkWriteUpsert> translateBulkWriteUpserts(final List<org.mongodb.BulkWriteUpsert> upserts,
                                                           final Decoder<DBObject> decoder) {
        List<BulkWriteUpsert> retVal = new ArrayList<BulkWriteUpsert>(upserts.size());
        for (org.mongodb.BulkWriteUpsert cur : upserts) {
            retVal.add(new com.mongodb.BulkWriteUpsert(cur.getIndex(), getUpsertedId(cur, decoder)));
        }
        return retVal;
    }

    private static Object getUpsertedId(final org.mongodb.BulkWriteUpsert cur, final Decoder<DBObject> decoder) {
        return decoder.decode(new BsonDocumentReader(new BsonDocument("_id", cur.getId())), DecoderContext.builder().build()).get("_id");
    }

    static BulkWriteException translateBulkWriteException(final org.mongodb.BulkWriteException e, final Decoder<DBObject> decoder) {
        return new BulkWriteException(translateBulkWriteResult(e.getWriteResult(), decoder), translateWriteErrors(e.getWriteErrors()),
                                      translateWriteConcernError(e.getWriteConcernError()), e.getServerAddress());
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

    static List<com.mongodb.operation.WriteRequest> translateWriteRequestsToNew(final List<WriteRequest> writeRequests,
                                                                                final Codec<DBObject> objectCodec) {
        List<com.mongodb.operation.WriteRequest> retVal = new ArrayList<com.mongodb.operation.WriteRequest>(writeRequests.size());
        for (WriteRequest cur : writeRequests) {
            retVal.add(cur.toNew());
        }
        return retVal;
    }

    private BulkWriteHelper() {
    }
}
