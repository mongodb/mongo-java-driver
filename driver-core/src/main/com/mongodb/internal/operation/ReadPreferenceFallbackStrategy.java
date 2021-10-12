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

package com.mongodb.internal.operation;

/**
 * An interface optionally implemented by a ReadOperation ....read preference with which this instance is configured, if all connected
 * servers
 * have
 * a maxWireVersion >= the given minWireVersion.  Otherwise, return a connection source that satisfied the given
 * fallbackReadPreference.
 *
 * This is useful for operations that are able to execute on a secondary on later server versions, but must execute on the primary on
 * earlier server versions.
 *
 * @see ReadOperation
 * @see com.mongodb.internal.operation.AggregateToCollectionOperation
 */
public interface ReadPreferenceFallbackStrategy {

    int getMinWireVersionToApplyReadPreference();
}
