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

package com.mongodb.connection;

import com.mongodb.async.SingleResultCallback;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

interface ConnectionPool extends Closeable {

    InternalConnection get();

    InternalConnection get(long timeout, TimeUnit timeUnit);

    void getAsync(SingleResultCallback<InternalConnection> callback);

    void invalidate();

    void close();
}
