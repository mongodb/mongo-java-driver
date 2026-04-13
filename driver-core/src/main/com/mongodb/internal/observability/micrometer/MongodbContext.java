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

package com.mongodb.internal.observability.micrometer;

import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.SenderContext;

/**
 * A MongoDB-specific {@link SenderContext} for Micrometer observations.
 * <p>
 * Extends {@link SenderContext} with {@link Kind#CLIENT} to preserve the client span kind
 * in the tracing bridge. Provides a MongoDB-specific type that users can filter on
 * when registering {@code ObservationHandler} or {@code ObservationConvention} instances.
 * </p>
 *
 * @since 5.7
 */
public class MongodbContext extends SenderContext<Object> {
    public MongodbContext() {
        super((carrier, key, value) -> { }, Kind.CLIENT);
    }
}
