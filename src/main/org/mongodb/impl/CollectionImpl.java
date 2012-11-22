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

import org.bson.io.PooledByteBufferOutput;
import org.mongodb.Collection;
import org.mongodb.MongoChannel;
import org.mongodb.MongoException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.WriteConcern;
import org.mongodb.protocol.MongoInsertMessage;
import org.mongodb.serialization.Serializer;

import java.io.IOException;

class CollectionImpl implements Collection {
    private final String name;
    private final DatabaseImpl database;

    public CollectionImpl(final String name, DatabaseImpl database) {
        this.name = name;
        this.database = database;
    }

    @Override
    public MongoClientImpl getMongoServer() {
        return getDatabase().getMongo();
    }

    @Override
    public DatabaseImpl getDatabase() {
        return database;
    }

    @Override
    public String getName() {
        return name;
    }

    public <T> void insert(T doc, WriteConcern writeConcern, Serializer serializer) {
        MongoInsertMessage insertMessage = new MongoInsertMessage(getFullName(), writeConcern,
                new PooledByteBufferOutput(getMongoServer().getBufferPool()));
        insertMessage.addDocument(doc.getClass(), doc, serializer);

        MongoChannel mongoChannel = null;
        try {
            mongoChannel = getMongoServer().getChannelPool().get();
            mongoChannel.sendMessage(insertMessage);
        } catch (InterruptedException e) {
            throw new MongoInterruptedException(e);
        } catch (IOException e) {
            throw new MongoException("insert", e);
        } finally {
            if (mongoChannel != null) {
                getMongoServer().getChannelPool().done(mongoChannel);
            }
        }
    }

    private String getFullName() {
        return getDatabase().getName() + "." + getName();
    }
}
