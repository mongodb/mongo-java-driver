/*
 * Copyright 2019-present MongoDB, Inc.
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
 *
 */

package com.mongodb.crypt.capi;

/**
 * The entry point to the MongoCrypt library.
 */
public class MongoCrypts {

    /**
     * Create a {@code MongoCrypt} instance.
     *
     * <p>
     * Make sure that JNA is able to find the shared library, most likely by setting the jna.library.path system property
     * </p>
     *
     * @param options the options
     * @return the instance
     */
    public static MongoCrypt create(MongoCryptOptions options) {
        return new MongoCryptImpl(options);
    }
}
