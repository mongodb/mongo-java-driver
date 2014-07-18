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

package com.mongodb.operation;

import org.mongodb.ServerCursor;

public class GetMore {
    private final int limit;
    private final int batchSize;
    private final long numFetchedSoFar;
    private final ServerCursor serverCursor;

    public GetMore(final ServerCursor serverCursor, final int limit, final int batchSize, final long numFetchedSoFar) {
        this.serverCursor = serverCursor;
        this.limit = limit;
        this.batchSize = batchSize;
        this.numFetchedSoFar = numFetchedSoFar;
    }

    public int getLimit() {
        return limit;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getNumFetchedSoFar() {
        return numFetchedSoFar;
    }

    public ServerCursor getServerCursor() {
        return serverCursor;
    }

    /**
     * Gets the limit of the number of documents in the OP_REPLY response to the get more request. A value of zero tells the server to use
     * the default size. A negative value tells the server to return no more than that number and immediately close the cursor. Otherwise,
     * the server will return no more than that number and return the same cursorId to allow the rest of the documents to be fetched, if it
     * turns out there are more documents.
     * <p/>
     * The value returned by this method is based on the limit, the batch size, both of which can be positive, negative, or zero, and the
     * number of documents fetched so far.
     *
     * @return the value for numberToReturn in the OP_GET_MORE wire protocol message.
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-get-more OP_GET_MORE
     */
    public int getNumberToReturn() {
        int numberToReturn;
        if (Math.abs(limit) != 0) {
            numberToReturn = Math.abs(limit) - (int) numFetchedSoFar;
            if (batchSize != 0 && numberToReturn > Math.abs(batchSize)) {
                numberToReturn = batchSize;
            }
        } else {
            numberToReturn = batchSize;
        }
        return numberToReturn;
    }
}
