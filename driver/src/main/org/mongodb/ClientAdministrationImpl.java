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

package org.mongodb;

import com.mongodb.operation.GetDatabaseNamesOperation;
import com.mongodb.operation.PingOperation;

import java.util.List;

import static com.mongodb.ReadPreference.primary;

/**
 * Contains the commands that can be run on MongoDB that do not require a database to be selected first.
 * These commands can be accessed via MongoClient.
 */
class ClientAdministrationImpl implements ClientAdministration {
    private final MongoClientImpl client;

    ClientAdministrationImpl(final MongoClientImpl client) {
        this.client = client;
    }

    @Override
    public double ping() {
        return client.execute(new PingOperation(), primary());
    }

    @Override
    public List<String> getDatabaseNames() {
        return client.execute(new GetDatabaseNamesOperation(), primary());
    }
}
