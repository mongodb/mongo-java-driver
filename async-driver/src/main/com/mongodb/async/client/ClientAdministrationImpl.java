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

package com.mongodb.async.client;

import com.mongodb.operation.GetDatabaseNamesOperation;
import com.mongodb.operation.PingOperation;
import org.mongodb.MongoFuture;

import java.util.List;

import static com.mongodb.ReadPreference.primary;

public class ClientAdministrationImpl implements ClientAdministration {

    private final MongoClientImpl client;

    ClientAdministrationImpl(final MongoClientImpl client) {
        this.client = client;
    }

    @Override
    public MongoFuture<Double> ping() {
        return client.execute(new PingOperation(), primary());
    }

    @Override
    public MongoFuture<List<String>> getDatabaseNames() {
        return client.execute(new GetDatabaseNamesOperation(), primary());
    }
}
