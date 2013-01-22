/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package com.google.code.morphia.query;

import com.mongodb.WriteResult;

public class UpdateResults<T> {
    private final WriteResult wr;

    public UpdateResults(final WriteResult wr) {
        this.wr = wr;
    }

    public String getError() {
        return wr.getLastError().getErrorMessage();
    }

    public boolean getHadError() {
        final String error = getError();
        return error != null && !error.isEmpty();
    }

    /**
     * @return true if updated, false if inserted or none effected
     */
    public boolean getUpdatedExisting() {
        return wr.getLastError().containsField("updatedExisting")
               ? (Boolean) wr.getLastError().get("updatedExisting") : false;
    }

    /**
     * @return number updated
     */
    public int getUpdatedCount() {
        return getUpdatedExisting() ? getN() : 0;
    }

    /**
     * @return number of affected documents
     */
    protected int getN() {
        return wr.getLastError().containsField("n") ? ((Number) wr.getLastError().get("n")).intValue() : 0;
    }

    /**
     * @return number inserted; this should be either 0/1.
     */
    public int getInsertedCount() {
        return !getUpdatedExisting() ? getN() : 0;
    }

    /**
     * @return the new _id field if an insert/upsert was performed
     */
    public Object getNewId() {
        return getInsertedCount() == 1 && wr.getLastError().containsField("upserted")
               ? wr.getLastError().get("upserted") : null;
    }

    /**
     * @return the underlying data
     */
    public WriteResult getWriteResult() {
        return wr;
    }
} 
