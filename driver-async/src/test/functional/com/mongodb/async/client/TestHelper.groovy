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

package com.mongodb.async.client

import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.session.ClientSession

import java.util.concurrent.TimeUnit

class TestHelper {
    static run(operation, ... args) {
        runOp(operation, 60, *args)
    }

    static runSlow(operation, ... args) {
        runOp(operation, 180, *args)
    }

    static runOp(operation, timeout, ... args) {
        FutureResultCallback futureResultCallback = new FutureResultCallback()
        List opArgs = (args != null) ? args : []
        operation.call(*opArgs + futureResultCallback)
        futureResultCallback.get(timeout, TimeUnit.SECONDS)
    }

    static <T> T execute(final Closure<T> method, final ClientSession session, ... args) {
        List opArgs = (args != null) ? args : []
        if (session != null) {
            opArgs = [session, *opArgs]
        }
        if (method.getParameterTypes().last() == SingleResultCallback) {
            runOp(method, 60, *opArgs)
        } else {
            method.call(*opArgs)
        }
    }

    static <T> void execute(final MongoIterable method) {
        method.batchCursor(new SingleResultCallback<AsyncBatchCursor>() {
            @Override
            void onResult(final AsyncBatchCursor result, final Throwable t) {
            }
        })
    }

    static <T> MongoIterable<T> createIterable(final Closure<T> method, ClientSession session, ... args) {
        List opArgs = (args != null) ? args : []
        if (session != null) {
            opArgs = [session, *opArgs]
        }
        method.call(*opArgs)
    }
}
