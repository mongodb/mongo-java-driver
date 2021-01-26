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

/**
 * This packages contains classes for the reactive stream client implementation.
 * <p>
 * All {@link org.reactivestreams.Publisher}s are <a href="https://projectreactor.io/docs/core/release/reference/#reactor.hotCold">cold</a>,
 * meaning that nothing happens until they are subscribed to.
 * So just creating a {@link org.reactivestreams.Publisher} won’t cause any network IO.
 * It’s not until {@link org.reactivestreams.Publisher#subscribe(org.reactivestreams.Subscriber)} is called that the driver executes the
 * operation.
 * <p>
 * All {@link org.reactivestreams.Publisher}s are unicast.
 * Each {@link org.reactivestreams.Subscription} to a {@link org.reactivestreams.Publisher} relates to a single MongoDB operation and its
 * {@link org.reactivestreams.Subscriber} will receive its own specific set of results.
 */
package com.mongodb.reactivestreams.client;
