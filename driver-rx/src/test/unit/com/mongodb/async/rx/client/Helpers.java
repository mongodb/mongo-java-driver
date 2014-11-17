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

package com.mongodb.async.rx.client;

import rx.Observable;

import java.util.ArrayList;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Helper class for asynchronous tests.
 */
final class Helpers {

    public static <T> T get(final Observable<T> observable) {
        return observable.timeout(90, SECONDS).first().toBlocking().first();
    }

    public static <T> List<T> toList(final Observable<T> observable) {
        return observable.timeout(90, SECONDS).toList().toBlocking().firstOrDefault(new ArrayList<T>());
    }

    private Helpers() {
    }
}