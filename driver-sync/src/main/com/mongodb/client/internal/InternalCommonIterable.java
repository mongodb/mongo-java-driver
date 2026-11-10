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

package com.mongodb.client.internal;

import com.mongodb.ExplainVerbosity;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.fail;

/**
 * The purpose of this interface is to ensure that all iterable API interfaces that share methods
 * have the signatures of those methods the same, and to help a maintainer to see which methods are shared.
 * See {@code com.mongodb.client.internal.InternalCommonIterableTest}.
 * This is a synchronous counterpart to {@code com.mongodb.reactivestreams.client.internal.InternalCommonBatchCursorPublisher}.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.
 */
public interface InternalCommonIterable<TResult> extends MongoIterable<TResult> {
    default MongoIterable<TResult> timeoutMode(TimeoutMode timeoutMode) {
        throw fail();
    }

    default void toCollection() {
        fail();
    }

    default MongoIterable<TResult> filter(@Nullable Bson filter) {
        throw fail();
    }

    default MongoIterable<TResult> limit(int limit) {
        throw fail();
    }

    default MongoIterable<TResult> skip(int skip) {
        throw fail();
    }

    default MongoIterable<TResult> maxTime(long maxTime, TimeUnit timeUnit) {
        throw fail();
    }

    default MongoIterable<TResult> maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit) {
        throw fail();
    }

    default MongoIterable<TResult> collation(@Nullable Collation collation) {
        throw fail();
    }

    default MongoIterable<TResult> sort(@Nullable Bson sort) {
        throw fail();
    }

    default MongoIterable<TResult> noCursorTimeout(boolean noCursorTimeout) {
        throw fail();
    }

    default MongoIterable<TResult> comment(@Nullable String comment) {
        throw fail();
    }

    default MongoIterable<TResult> comment(@Nullable BsonValue comment) {
        throw fail();
    }

    default MongoIterable<TResult> hint(@Nullable Bson hint) {
        throw fail();
    }

    default MongoIterable<TResult> hintString(@Nullable String hint) {
        throw fail();
    }

    default MongoIterable<TResult> let(@Nullable Bson variables) {
        throw fail();
    }

    default MongoIterable<TResult> allowDiskUse(@Nullable Boolean allowDiskUse) {
        throw fail();
    }

    default Document explain() {
        throw fail();
    }

    default Document explain(ExplainVerbosity verbosity) {
        throw fail();
    }

    default <E> E explain(Class<E> explainResultClass) {
        throw fail();
    }

    default <E> E explain(Class<E> explainResultClass, ExplainVerbosity verbosity) {
        throw fail();
    }

    default MongoIterable<TResult> bypassDocumentValidation(@Nullable Boolean bypassDocumentValidation) {
        throw fail();
    }

    default MongoIterable<TResult> commandOption(String name, Object value) {
        throw fail();
    }
}
