/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.protocol;

import org.mongodb.io.ChannelAwareOutputBuffer;
import org.mongodb.operation.MongoQuery;

public abstract class MongoQueryBaseMessage extends MongoRequestMessage {

    public MongoQueryBaseMessage(final String collectionName) {
        super(collectionName, OpCode.OP_QUERY);
    }

    // TODO: options?
    protected void writeQueryPrologue(final MongoQuery query, final ChannelAwareOutputBuffer buffer) {
        int allOptions = 0;
        if (query.getReadPreference().isSlaveOk()) {
            allOptions |= QueryOptions.SLAVEOK;
        }

        buffer.writeInt(allOptions);
        buffer.writeCString(getCollectionName());

        buffer.writeInt(query.getSkip());
        buffer.writeInt(chooseBatchSize(query.getBatchSize(), query.getLimit(), 0));
    }

    // TODO: test this, extensively
    private int chooseBatchSize(final int batchSize, final int limit, final int fetched) {
        final int bs = Math.abs(batchSize);
        final int remaining = limit > 0 ? limit - fetched : 0;
        int res;
        if (bs == 0 && remaining > 0) {
            res = remaining;
        }
        else if (bs > 0 && remaining == 0) {
            res = bs;
        }
        else {
            res = Math.min(bs, remaining);
        }

        if (batchSize < 0) {
            // force close
            res = -res;
        }

        return res;
    }
}
