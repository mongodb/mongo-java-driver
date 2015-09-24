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

package com.mongodb;

import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.WriteOperation;

/**
 * A performance monitor to track execution times.
 *
 * @since 3.1
 */
public interface IMongoPerformanceMonitor {
    /**
     * Tracks execution time from startTime until now for operation.
     *
     * @param <T> the operations result type.
     * @param operation to track execution time of
     * @param startTime of operation in nanoseconds
     */
    <T> void track(ReadOperation<T> operation, long startTime);

    /**
     * Tracks execution time from startTime until now for operation.
     *
     * @param <T> the operations result type.
     * @param operation to track execution time of
     * @param startTime of operation in nanoseconds
     */
    <T> void track(WriteOperation<T> operation, long startTime);
}
