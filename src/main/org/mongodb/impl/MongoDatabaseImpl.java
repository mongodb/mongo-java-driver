/**
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

package org.mongodb.impl;

import org.mongodb.CommandResult;
import org.mongodb.MongoClient;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoDocument;

class MongoDatabaseImpl implements MongoDatabase {
    private final MongoClient mongo;
    private final String name;

    public MongoDatabaseImpl(final String name, final MongoClient mongo) {
        this.mongo = mongo;
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    public MongoCollectionImpl<MongoDocument> getCollection(final String name) {
        return new MongoCollectionImpl<MongoDocument>(name, this);
    }

    @Override
    public CommandResult executeCommand(final MongoDocument command) {
         return mongo.getOperations().executeCommand(getName(), command);
    }

    @Override
    public MongoClient getMongoClient() {
        return mongo;
    }
}
