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

package com.mongodb.internal.connection;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.bulk.WriteConcernError;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparingInt;

/**
 * This class is not part of the public API.  It may be changed or removed at any time.
 */
public class BulkWriteBatchCombiner {
    private final ServerAddress serverAddress;
    private final boolean ordered;
    private final WriteConcern writeConcern;

    private int insertedCount;
    private int matchedCount;
    private int deletedCount;
    private int modifiedCount = 0;
    private final Set<BulkWriteUpsert> writeUpserts = new TreeSet<>(comparingInt(BulkWriteUpsert::getIndex));
    private final Set<BulkWriteInsert> writeInserts = new TreeSet<>(comparingInt(BulkWriteInsert::getIndex));
    private final Set<BulkWriteError> writeErrors = new TreeSet<>(comparingInt(BulkWriteError::getIndex));
    private final Set<String> errorLabels = new HashSet<>();
    private final List<WriteConcernError> writeConcernErrors = new ArrayList<>();

    /**
     * Construct an instance.
     *
     * @param serverAddress the server address
     * @param ordered       ordered
     * @param writeConcern  the write concern
     */
    public BulkWriteBatchCombiner(final ServerAddress serverAddress, final boolean ordered, final WriteConcern writeConcern) {
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.ordered = ordered;
        this.serverAddress = notNull("serverAddress", serverAddress);
    }

    /**
     * Add a result
     *
     * @param result   the result
     */
    public void addResult(final BulkWriteResult result) {
        insertedCount += result.getInsertedCount();
        matchedCount += result.getMatchedCount();
        deletedCount += result.getDeletedCount();
        modifiedCount += result.getModifiedCount();
        writeUpserts.addAll(result.getUpserts());
        writeInserts.addAll(result.getInserts());
    }

    /**
     * Add an error result
     *
     * @param exception the exception
     * @param indexMap  the index map
     */
    public void addErrorResult(final MongoBulkWriteException exception, final IndexMap indexMap) {
        addResult(exception.getWriteResult());
        errorLabels.addAll(exception.getErrorLabels());
        mergeWriteErrors(exception.getWriteErrors(), indexMap);
        mergeWriteConcernError(exception.getWriteConcernError());
    }

    /**
     * Add a write error result
     *
     * @param writeError the write error
     * @param indexMap   the index map
     */
    public void addWriteErrorResult(final BulkWriteError writeError, final IndexMap indexMap) {
        notNull("writeError", writeError);
        mergeWriteErrors(singletonList(writeError), indexMap);
    }

    /**
     * Add a write concern error result
     *
     * @param writeConcernError the write concern error
     */
    public void addWriteConcernErrorResult(final WriteConcernError writeConcernError) {
        notNull("writeConcernError", writeConcernError);
        mergeWriteConcernError(writeConcernError);
    }

    /**
     * Add a list of error results and a write concern error
     *
     * @param writeErrors       the errors
     * @param writeConcernError the write concern error
     * @param indexMap          the index map
     */
    public void addErrorResult(final List<BulkWriteError> writeErrors,
                               final WriteConcernError writeConcernError, final IndexMap indexMap) {
        mergeWriteErrors(writeErrors, indexMap);
        mergeWriteConcernError(writeConcernError);
    }

    /**
     * Gets the combined result.
     *
     * @return the result
     */
    public BulkWriteResult getResult() {
        throwOnError();
        return createResult();
    }

    /**
     * True if ordered and has write errors.
     *
     * @return true if no more batches should be sent
     */
    public boolean shouldStopSendingMoreBatches() {
        return ordered && hasWriteErrors();
    }

    /**
     * Gets whether there are errors in the combined result.
     *
     * @return whether there are errors in the combined result
     */
    public boolean hasErrors() {
        return hasWriteErrors() || hasWriteConcernErrors();
    }

    /**
     * Gets the combined errors as an exception
     * @return the bulk write exception, or null if there were no errors
     */
    public MongoBulkWriteException getError() {
        if (!hasErrors()) {
            return null;
        }
        return new MongoBulkWriteException(createResult(), new ArrayList<>(writeErrors),
                writeConcernErrors.isEmpty() ? null : writeConcernErrors.get(writeConcernErrors.size() - 1),
                serverAddress, errorLabels);
    }

    @SuppressWarnings("deprecation")
    private void mergeWriteConcernError(final WriteConcernError writeConcernError) {
        if (writeConcernError != null) {
            if (writeConcernErrors.isEmpty()) {
                writeConcernErrors.add(writeConcernError);
                errorLabels.addAll(writeConcernError.getErrorLabels());
            } else if (!writeConcernError.equals(writeConcernErrors.get(writeConcernErrors.size() - 1))) {
                writeConcernErrors.add(writeConcernError);
                errorLabels.addAll(writeConcernError.getErrorLabels());
            }
        }
    }

    private void mergeWriteErrors(final List<BulkWriteError> newWriteErrors, final IndexMap indexMap) {
        for (BulkWriteError cur : newWriteErrors) {
            writeErrors.add(new BulkWriteError(cur.getCode(), cur.getMessage(), cur.getDetails(), indexMap.map(cur.getIndex())));
        }
    }

    private void throwOnError() {
        if (hasErrors()) {
            throw getError();
        }
    }

    private BulkWriteResult createResult() {
        return writeConcern.isAcknowledged()
               ? BulkWriteResult.acknowledged(insertedCount, matchedCount, deletedCount, modifiedCount,
                                              new ArrayList<>(writeUpserts), new ArrayList<>(writeInserts))
               : BulkWriteResult.unacknowledged();
    }

    private boolean hasWriteErrors() {
        return !writeErrors.isEmpty();
    }

    private boolean hasWriteConcernErrors() {
        return !writeConcernErrors.isEmpty();
    }
}
