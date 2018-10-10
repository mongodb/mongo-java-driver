/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.internal;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;

import java.util.function.Consumer;

final class Java8ForEachHelper {

    static <TResult> void forEach(final MongoIterable<TResult> iterable, final Consumer<? super TResult> block) {
        MongoCursor<TResult> cursor = iterable.iterator();
        try {
            while (cursor.hasNext()) {
                block.accept(cursor.next());
            }
        } finally {
            cursor.close();
        }
    }

    private Java8ForEachHelper() {
    }
}
