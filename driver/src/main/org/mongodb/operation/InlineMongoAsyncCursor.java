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

package org.mongodb.operation;

import org.mongodb.AsyncBlock;
import org.mongodb.MongoAsyncCursor;

import java.util.Iterator;
import java.util.List;

class InlineMongoAsyncCursor<T> implements MongoAsyncCursor<T> {
    private final Iterator<T> iterator;

    public InlineMongoAsyncCursor(final List<T> results) {
        iterator = results.iterator();
    }

    public void close() {
    }

    private boolean hasNext() {
        return iterator.hasNext();
    }

    private T next() {
        return iterator.next();
    }

    @Override
    public void start(final AsyncBlock<? super T> block) {
        while (hasNext()) {
            block.apply(next());
        }
        block.done();
    }
}
