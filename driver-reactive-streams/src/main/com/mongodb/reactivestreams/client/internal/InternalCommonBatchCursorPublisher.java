/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.ExplainVerbosity;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.fail;

/**
 * The purpose of this interface is to ensure that all cursor publisher API interfaces that share methods
 * have the signatures of those methods the same, and to help a maintainer to see which methods are shared.
 * See {@code com.mongodb.reactivestreams.client.internal.InternalCommonBatchCursorPublisherTest}.
 * This is a reactive counterpart to {@code com.mongodb.client.internal.InternalCommonIterable}.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.
 */
public interface InternalCommonBatchCursorPublisher<TResult> extends Publisher<TResult> {
    default Publisher<TResult> first() {
        throw fail();
    }

    default Publisher<TResult> batchSize(int batchSize) {
        throw fail();
    }

    default Publisher<TResult> timeoutMode(TimeoutMode timeoutMode) {
        throw fail();
    }

    default Publisher<Void> toCollection() {
        throw fail();
    }

    default Publisher<TResult> filter(@Nullable Bson filter) {
        throw fail();
    }

    default Publisher<TResult> limit(int limit) {
        throw fail();
    }

    default Publisher<TResult> skip(int skip) {
        throw fail();
    }

    default Publisher<TResult> maxTime(long maxTime, TimeUnit timeUnit) {
        throw fail();
    }

    default Publisher<TResult> maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit) {
        throw fail();
    }

    default Publisher<TResult> collation(@Nullable Collation collation) {
        throw fail();
    }

    default Publisher<TResult> sort(@Nullable Bson sort) {
        throw fail();
    }

    default Publisher<TResult> noCursorTimeout(boolean noCursorTimeout) {
        throw fail();
    }

    default Publisher<TResult> comment(@Nullable String comment) {
        throw fail();
    }

    default Publisher<TResult> comment(@Nullable BsonValue comment) {
        throw fail();
    }

    default Publisher<TResult> hint(@Nullable Bson hint) {
        throw fail();
    }

    default Publisher<TResult> hintString(@Nullable String hint) {
        throw fail();
    }

    default Publisher<TResult> let(@Nullable Bson variables) {
        throw fail();
    }

    default Publisher<TResult> allowDiskUse(@Nullable Boolean allowDiskUse) {
        throw fail();
    }

    default Publisher<Document> explain() {
        throw fail();
    }

    default Publisher<Document> explain(ExplainVerbosity verbosity) {
        throw fail();
    }

    default <E> Publisher<E> explain(Class<E> explainResultClass) {
        throw fail();
    }

    default <E> Publisher<E> explain(Class<E> explainResultClass, ExplainVerbosity verbosity) {
        throw fail();
    }

    default Publisher<TResult> bypassDocumentValidation(@Nullable Boolean bypassDocumentValidation) {
        throw fail();
    }

    default Publisher<TResult> commandOption(String name, Object value) {
        throw fail();
    }
}
