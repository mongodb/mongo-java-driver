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

package com.mongodb.async.client;

import com.mongodb.MongoException;
import com.mongodb.async.FutureResultCallback;

import java.util.concurrent.TimeUnit;

public abstract class MongoOperation<TResult> {
    private FutureResultCallback<TResult> callback = new FutureResultCallback<TResult>();

    public FutureResultCallback<TResult> getCallback() {
        return callback;
    }

    public TResult get() {
        execute();
        try {
            return callback.get(60, TimeUnit.SECONDS);
        } catch (MongoException e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public abstract void execute();
}
