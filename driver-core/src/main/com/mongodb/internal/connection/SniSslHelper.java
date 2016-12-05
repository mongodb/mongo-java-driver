/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.internal.connection;

import com.mongodb.ServerAddress;

import javax.net.ssl.SSLParameters;

interface SniSslHelper {

    /**
     * Enable SNI.
     *
     * @param address       the server address
     * @param sslParameters the SSL parameters
     */
    void enableSni(final ServerAddress address, final SSLParameters sslParameters);
}
