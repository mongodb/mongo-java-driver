/*
 * Copyright 2015 MongoDB, Inc.
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

import com.mongodb.reactivestreams.client.Success;


@SuppressWarnings("deprecation")
final class PublisherHelper {

    /**
     * Helper to trigger Boolean SingleResultCallbacks for Void operations
     *
     * @param callback the boolean single result callback.
     * @return the results callback for an operation that returns null to signal success.
     */
    public static com.mongodb.async.SingleResultCallback<Void> voidToSuccessCallback(
            final com.mongodb.async.SingleResultCallback<Success> callback) {
        return new com.mongodb.async.SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                callback.onResult(Success.SUCCESS, t);
            }
        };
    }

    private PublisherHelper() {
    }

}
