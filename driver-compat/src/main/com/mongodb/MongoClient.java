/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package com.mongodb;

import java.net.UnknownHostException;

public class MongoClient extends Mongo {
    /**
     * Creates an instance based on a (single) mongodb node (localhost, default port).
     *
     * @throws java.net.UnknownHostException
     * @throws MongoException
     */
    public MongoClient() throws UnknownHostException {
        this(new ServerAddress());
    }

    public MongoClient(final ServerAddress serverAddress) {
        super(serverAddress);
    }
}
