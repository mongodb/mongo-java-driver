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

package com.mongodb.client.internal

import com.mongodb.client.ClientSession
import com.mongodb.internal.ClientSideOperationTimeoutFactories
import com.mongodb.internal.ClientSideOperationTimeoutFactory

class TestHelper {

    public static final ClientSideOperationTimeoutFactory CSOT_FACTORY_TIMEOUT =
            ClientSideOperationTimeoutFactories.create(60000)
    public static final ClientSideOperationTimeoutFactory CSOT_FACTORY_MAX_TIME =
            ClientSideOperationTimeoutFactories.create(null, 99, 0, 0)
    public static final ClientSideOperationTimeoutFactory CSOT_FACTORY_MAX_AWAIT_TIME =
            ClientSideOperationTimeoutFactories.create(null, 0, 999)
    public static final ClientSideOperationTimeoutFactory CSOT_FACTORY_MAX_TIME_AND_MAX_AWAIT_TIME =
            ClientSideOperationTimeoutFactories.create(null, 99, 999)
    public static final ClientSideOperationTimeoutFactory CSOT_FACTORY_NO_TIMEOUT =
            ClientSideOperationTimeoutFactories.NO_TIMEOUT

    static <T> T execute(final Closure<T> method, final ClientSession session, ... restOfArgs) {
        if (session == null) {
            method.call(restOfArgs)
        }  else {
            method.call([session, *restOfArgs] as Object[])
        }
    }
}
