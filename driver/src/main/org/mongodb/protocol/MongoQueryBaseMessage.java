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

    protected void writeQueryPrologue(final MongoQuery query, final ChannelAwareOutputBuffer buffer) {
        buffer.writeInt(query.getFlags());
        buffer.writeCString(getCollectionName());

        buffer.writeInt(query.getSkip());
        buffer.writeInt(chooseNumberToReturn(query.getBatchSize(), query.getLimit()));
    }

    // TODO: test this, extensively
    private int chooseNumberToReturn(final int batchSize, final int limit) {
        final int bs = Math.abs(batchSize);
        final int lm = Math.abs(limit);
        int res = bs * lm != 0 ? Math.min(bs, lm) : bs + lm;

        if (Math.min(batchSize, limit) < 0) {
            // force close
            res = -res;
        }

        return res;
    }
}
