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

package com.mongodb.reactivestreams.client;

import com.mongodb.lang.Nullable;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

/**
 * This interface is shared by {@link ListCollectionsPublisher}, {@link ListCollectionNamesPublisher}.
 * Each of those interfaces may have additional methods specific to it.
 */
interface CommonListCollectionsPublisher<TResult> extends Publisher<TResult> {
    CommonListCollectionsPublisher<TResult> filter(@Nullable Bson filter);

    CommonListCollectionsPublisher<TResult> maxTime(long maxTime, TimeUnit timeUnit);

    CommonListCollectionsPublisher<TResult> batchSize(int batchSize);

    CommonListCollectionsPublisher<TResult> comment(@Nullable String comment);

    CommonListCollectionsPublisher<TResult> comment(@Nullable BsonValue comment);

    Publisher<TResult> first();
}
