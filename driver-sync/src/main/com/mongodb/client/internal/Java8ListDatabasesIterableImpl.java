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

import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.lang.Nullable;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.function.Consumer;

class Java8ListDatabasesIterableImpl<TResult> extends ListDatabasesIterableImpl<TResult> {
    Java8ListDatabasesIterableImpl(final @Nullable ClientSession clientSession, final Class<TResult> resultClass,
                                   final CodecRegistry codecRegistry, final ReadPreference readPreference,
                                   final OperationExecutor executor) {
        super(clientSession, resultClass, codecRegistry, readPreference, executor);
    }


    @Override
    public void forEach(final Consumer<? super TResult> action) {
        Java8ForEachHelper.forEach(this, action);
    }
}
